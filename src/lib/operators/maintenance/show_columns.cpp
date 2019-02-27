#include "show_columns.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

#include "constant_mappings.hpp"

namespace opossum {

ShowColumns::ShowColumns(const std::string& table_name)
    : AbstractReadOnlyOperator(OperatorType::ShowColumns), _table_name(table_name) {}

const std::string ShowColumns::name() const { return "ShowColumns"; }

std::shared_ptr<AbstractOperator> ShowColumns::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ShowColumns>(_table_name);
}

void ShowColumns::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ShowColumns::_on_execute() {
  TableColumnDefinitions column_definitions;
  column_definitions.emplace_back("column_name", DataType::String);
  column_definitions.emplace_back("column_type", DataType::String);
  column_definitions.emplace_back("is_nullable", DataType::Int);
  auto out_table = std::make_shared<Table>(column_definitions, TableType::Data);

  const auto table = StorageManager::get().get_table(_table_name);
  Segments segments;

  const auto& column_names = table->column_names();
  const auto vs_names = std::make_shared<ValueSegment<pmr_string>>(
      tbb::concurrent_vector<pmr_string>(column_names.begin(), column_names.end()));
  segments.push_back(vs_names);

  const auto& column_types = table->column_data_types();

  auto column_types_as_string = tbb::concurrent_vector<pmr_string>{};
  for (const auto column_type : column_types) {
    column_types_as_string.push_back(pmr_string{data_type_to_string.left.at(column_type)});
  }

  const auto vs_types = std::make_shared<ValueSegment<pmr_string>>(std::move(column_types_as_string));
  segments.push_back(vs_types);

  const auto& column_nullables = table->columns_are_nullable();
  const auto vs_nullables = std::make_shared<ValueSegment<int32_t>>(
      tbb::concurrent_vector<int32_t>(column_nullables.begin(), column_nullables.end()));
  segments.push_back(vs_nullables);

  out_table->append_chunk(segments);

  return out_table;
}

}  // namespace opossum
