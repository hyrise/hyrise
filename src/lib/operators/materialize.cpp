#include "materialize.hpp"

#include "resolve_type.hpp"
#include "storage/base_column.hpp"
#include "storage/chunk.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/table.hpp"

namespace opossum {

Materialize::Materialize(const std::shared_ptr<const AbstractOperator> in)
    : AbstractReadOnlyOperator{in} {}

const std::string Materialize::name() const { return "Materialize"; }
const std::string Materialize::description(DescriptionMode description_mode) const { return name(); }

std::shared_ptr<AbstractOperator> Materialize::recreate(const std::vector<AllParameterVariant>& args) const {
  return std::make_shared<Materialize>(_input_left->recreate(args));
}

std::shared_ptr<const Table> Materialize::_on_execute() {
  const auto in_table = _input_left->get_output();

  auto out_table = Table::create_with_layout_from(in_table);

  for (auto chunk_id = ChunkID{0u}; chunk_id < in_table->chunk_count(); ++chunk_id) {
    const auto in_chunk = in_table->get_chunk(chunk_id);
    auto out_chunk = std::make_shared<Chunk>();

    for (auto column_id = ColumnID{0u}; column_id < in_chunk->column_count(); ++column_id) {
      const auto in_base_column = in_chunk->get_column(column_id);
      const auto data_type = in_table->column_type(column_id);

      auto out_base_column = std::shared_ptr<BaseColumn>{};

      resolve_data_type(data_type, [&](auto data_type_t) {
        using DataTypeT = typename decltype(data_type_t)::type;

        auto values = pmr_concurrent_vector<DataTypeT>{};
        values.reserve(in_base_column->size());

        auto null_values = pmr_concurrent_vector<bool>{};
        null_values.reserve(in_base_column->size());

        resolve_column_type<DataTypeT>(*in_base_column, [&](const auto& in_column) {
          auto iterable = create_iterable_from_column<DataTypeT>(in_column);
          // iterable.set_allow_reordering(true);

          iterable.for_each([&](const auto& value) {
            values.push_back(value.value());
            null_values.push_back(value.is_null());
          });
        });

        out_base_column = std::make_shared<ValueColumn<DataTypeT>>(std::move(values), std::move(null_values));
      });

      out_chunk->add_column(out_base_column);
    }

    out_table->emplace_chunk(out_chunk);
  }

  return out_table;
}


}  // namespace opossum
