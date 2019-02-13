#include "table_materialize.hpp"

#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "resolve_type.hpp"

namespace opossum {

TableMaterialize::TableMaterialize(const std::shared_ptr<const AbstractOperator>& in):
  AbstractReadOnlyOperator(OperatorType::TableMaterialize, in) {

}

const std::string TableMaterialize::name() const {
  return "TableMaterialize";
}

const std::string TableMaterialize::description(DescriptionMode description_mode) const {
  return name();
}

std::shared_ptr<const Table> TableMaterialize::_on_execute() {
  const auto output_table = std::make_shared<Table>(input_table_left()->column_definitions(), TableType::Data, std::nullopt, UseMvcc::No);

  Segments segments;

  for (auto column_id = ColumnID{0}; column_id < input_table_left()->column_count(); ++column_id) {
    resolve_data_type(input_table_left()->column_data_type(column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      auto values = pmr_concurrent_vector<ColumnDataType>(input_table_left()->row_count());
      auto null_values = pmr_concurrent_vector<bool>();
      if (input_table_left()->column_is_nullable(column_id)) {
        null_values.resize(input_table_left()->row_count());
      }

      auto row_idx = size_t{0};

      for (const auto& chunk : input_table_left()->chunks()) {
        if (input_table_left()->column_is_nullable(column_id)) {
          segment_iterate<ColumnDataType>(*chunk->get_segment(column_id), [&](const auto& iterator_value) {
            values[row_idx] = iterator_value.value();
            null_values[row_idx] = iterator_value.is_null();
            ++row_idx;
          });
        } else {
          segment_iterate<ColumnDataType>(*chunk->get_segment(column_id), [&](const auto& iterator_value) {
            values[row_idx] = iterator_value.value();
            ++row_idx;
          });
        }
      }

      if (input_table_left()->column_is_nullable(column_id)) {
        segments.emplace_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(null_values)));
      } else {
        segments.emplace_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values)));
      }

    });
  }

  output_table->append_chunk(segments);

  return output_table;
}

std::shared_ptr<AbstractOperator> TableMaterialize::_on_deep_copy(
const std::shared_ptr<AbstractOperator>& copied_input_left,
const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<TableMaterialize>(copied_input_left);
}

void TableMaterialize::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {
  // No params
}

}  // namespace opossum
