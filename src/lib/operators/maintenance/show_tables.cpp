#include "show_tables.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

ShowTables::ShowTables() : AbstractReadOnlyOperator(OperatorType::ShowTables) {}

const std::string ShowTables::name() const { return "ShowTables"; }

std::shared_ptr<AbstractOperator> ShowTables::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_input_left,
    const std::shared_ptr<AbstractOperator>& copied_input_right) const {
  return std::make_shared<ShowTables>();
}

void ShowTables::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

std::shared_ptr<const Table> ShowTables::_on_execute() {
  auto table = std::make_shared<Table>(TableColumnDefinitions{{"table_name", DataType::String}}, TableType::Data);

  const auto table_names = StorageManager::get().table_names();
  const auto segment = std::make_shared<ValueSegment<pmr_string>>(
      tbb::concurrent_vector<pmr_string>(table_names.begin(), table_names.end()));

  Segments segments;
  segments.push_back(segment);
  table->append_chunk(segments);

  return table;
}

}  // namespace opossum
