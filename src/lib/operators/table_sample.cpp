#include "table_sample.hpp"

#include <string>

#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"

namespace opossum {

TableSample::TableSample(const std::shared_ptr<const AbstractOperator>& in, const size_t num_rows):
  AbstractReadOnlyOperator(OperatorType::TableSample), _num_rows(num_rows) {

}

const std::string TableSample::name() const {
  return "TableSample";
}

const std::string TableSample::description(DescriptionMode description_mode) const {
  return name() + ": " + std::to_string(_num_rows);
}

std::shared_ptr<const Table> TableSample::_on_execute() {
  const auto output_table = std::make_shared<Table>(input_table_left()->column_definitions(), TableType::Data, std::nullopt, UseMvcc::No);

  if (_num_rows == 0) {
    return output_table;
  }

  if (_num_rows >= input_table_left()->row_count()) {
    return input_table_left();
  }

  const auto step = input_table_left()->row_count() / _num_rows;
  const auto remainder = input_table_left()->row_count() % _num_rows;
  const auto column_count = input_table_left()->column_count();

  auto chunk_offset = size_t{step};
  // Magic variable that gets increased by `remainder` per step
  auto k = size_t{0};

  for (auto chunk_id = ChunkID{0}; chunk_id < input_table_left()->chunk_count(); ++chunk_id) {
    const auto& chunk = input_table_left()->get_chunk(chunk_id);
    auto pos_list = PosList{};

    const auto chunk_size = chunk->size();
    for(; chunk_offset < chunk_size; ) {
      pos_list.emplace_back(chunk_id, chunk_offset);

      chunk_offset += step;
      k += remainder;

      if (k >= step) {
        ++chunk_offset;
        k -= step;
      }
    }

    chunk_offset -= chunk_size;

    auto segments = Segments{input_table_left()->column_count()};

    for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
      resolve_data_type(input_table_left()->column_data_type(column_id), [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        auto values = std::vector<ColumnDataType>{};
        values.reserve(pos_list.size());
        auto null_values = std::vector<bool>{};

        const auto is_nullable = input_table_left()->column_is_nullable(column_id);

        if (is_nullable) {
          null_values.reserve(pos_list.size());
        }

        segment_with_iterators_filtered<ColumnDataType>(chunk->get_segment(column_id), pos_list, [&](const auto& iterator_value) {
          values.emplace_back(iterator_value.value());

          if (is_nullable) {
            null_values.emplace_back(iterator_value.is_null());
          }
        });

        if (is_nullable) {
          segments.emplace_back(std::make_sh)
        }
      });
    }
  }


}

std::shared_ptr<AbstractOperator> TableSample::_on_deep_copy(
const std::shared_ptr<AbstractOperator>& copied_input_left,
const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableSample>(copied_input_left, _num_rows);
}

void TableSample::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No params
}

}  // namespace opossum