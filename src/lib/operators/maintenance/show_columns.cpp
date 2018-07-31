#include "show_columns.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_column.hpp"

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
  ChunkColumns columns;

  const auto& column_names = table->column_names();
  const auto vc_names = std::make_shared<ValueColumn<std::string>>(
      tbb::concurrent_vector<std::string>(column_names.begin(), column_names.end()));
  columns.push_back(vc_names);

  const auto& column_types = table->column_data_types();

  auto data_types = tbb::concurrent_vector<std::string>{};
  for (const auto column_type : column_types) {
    data_types.push_back(data_type_to_string.left.at(column_type));
  }

  const auto vc_types = std::make_shared<ValueColumn<std::string>>(std::move(data_types));
  columns.push_back(vc_types);

  const auto& column_nullables = table->columns_are_nullable();
  const auto vc_nullables = std::make_shared<ValueColumn<int32_t>>(
      tbb::concurrent_vector<int32_t>(column_nullables.begin(), column_nullables.end()));
  columns.push_back(vc_nullables);

  out_table->append_chunk(columns);

  return out_table;
}

}  // namespace opossum
