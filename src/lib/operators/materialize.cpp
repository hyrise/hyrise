#include "materialize.hpp"

#include <mutex>

#include "resolve_type.hpp"
#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"

namespace opossum {

Materialize::Materialize(const std::shared_ptr<const AbstractOperator> in)
    : AbstractReadOnlyOperator{OperatorType::Materialize, in} {}

const std::string Materialize::name() const { return "Materialize"; }
const std::string Materialize::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> Materialize::_on_recreate(
    const std::vector<AllParameterVariant>& args, const std::shared_ptr<AbstractOperator>& recreated_input_left,
    const std::shared_ptr<AbstractOperator>& recreated_input_right) const {
  return std::make_shared<Materialize>(recreated_input_left);
}

std::shared_ptr<const Table> Materialize::_on_execute() {
  const auto in_table = _input_left->get_output();

  auto out_table = std::make_shared<Table>(in_table->column_definitions(), TableType::Data);

  std::mutex output_mutex{};

  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(in_table->chunk_count());

  for (ChunkID chunk_id{0u}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    auto job_task = std::make_shared<JobTask>([=, &output_mutex]() {

      const auto in_chunk = in_table->get_chunk(chunk_id);

      // The new chunk is allocated on the same node
      const auto& alloc = in_chunk->get_allocator();

      ChunkColumns out_columns{alloc};
      out_columns.reserve(in_chunk->column_count());

      for (ColumnID column_id{0u}; column_id < in_chunk->column_count(); ++column_id) {
        const auto in_base_column = in_chunk->get_mutable_column(column_id);
        const auto data_type = in_table->column_data_type(column_id);

        if (std::dynamic_pointer_cast<BaseValueColumn>(in_base_column) != nullptr) {
          out_columns.push_back(in_base_column);
          continue;
        }

        resolve_data_type(data_type, [&](auto data_type_c) {
          using DataT = typename decltype(data_type_c)::type;

          auto values = pmr_concurrent_vector<DataT>{alloc};
          values.reserve(in_base_column->size());

          auto null_values = pmr_concurrent_vector<bool>{alloc};
          null_values.reserve(in_base_column->size());

          resolve_column_type<DataT>(*in_base_column, [&](const auto& in_column) {
            auto iterable = create_iterable_from_column<DataT>(in_column);

            iterable.for_each([&](const auto& value) {
              values.push_back(value.value());
              null_values.push_back(value.is_null());
            });
          });

          auto out_base_column = std::make_shared<ValueColumn<DataT>>(std::move(values), std::move(null_values));
          out_columns.push_back(out_base_column);
        });
      }

      std::lock_guard<std::mutex> lock(output_mutex);
      out_table->append_chunk(std::move(out_columns), alloc);
    });

    jobs.push_back(job_task);
    job_task->schedule();
  }

  CurrentScheduler::wait_for_tasks(jobs);

  return out_table;
}

}  // namespace opossum
