#include "show_cxlumns.hpp"

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

ShowCxlumns::ShowCxlumns(const std::string& table_name)
    : AbstractReadOnlyOperator(OperatorType::ShowCxlumns), _table_name(table_name) {}

const std::string ShowCxlumns::name() const { return "ShowCxlumns"; }

std::shared_ptr<AbstractOperator> ShowCxlumns::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ShowCxlumns>(_table_name);
}

void ShowCxlumns::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ShowCxlumns::_on_execute() {
  TableCxlumnDefinitions cxlumn_definitions;
  cxlumn_definitions.emplace_back("cxlumn_name", DataType::String);
  cxlumn_definitions.emplace_back("cxlumn_type", DataType::String);
  cxlumn_definitions.emplace_back("is_nullable", DataType::Int);
  auto out_table = std::make_shared<Table>(cxlumn_definitions, TableType::Data);

  const auto table = StorageManager::get().get_table(_table_name);
  ChunkSegments columns;

  const auto& cxlumn_names = table->cxlumn_names();
  const auto vc_names = std::make_shared<ValueSegment<std::string>>(
      tbb::concurrent_vector<std::string>(cxlumn_names.begin(), cxlumn_names.end()));
  columns.push_back(vc_names);

  const auto& cxlumn_types = table->cxlumn_data_types();

  auto data_types = tbb::concurrent_vector<std::string>{};
  for (const auto cxlumn_type : cxlumn_types) {
    data_types.push_back(data_type_to_string.left.at(cxlumn_type));
  }

  const auto vc_types = std::make_shared<ValueSegment<std::string>>(std::move(data_types));
  columns.push_back(vc_types);

  const auto& column_nullables = table->cxlumns_are_nullable();
  const auto vc_nullables = std::make_shared<ValueSegment<int32_t>>(
      tbb::concurrent_vector<int32_t>(column_nullables.begin(), column_nullables.end()));
  columns.push_back(vc_nullables);

  out_table->append_chunk(columns);

  return out_table;
}

}  // namespace opossum
