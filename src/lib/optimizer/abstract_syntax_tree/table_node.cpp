#include "table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

const std::string TableNode::description() const { return "Table: " + _table_name; }

const std::vector<std::string> TableNode::output_columns() {
  // Cache call to StorageManager.
  if (_column_names.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    _column_names = table->column_names();
  }

  return _column_names;
}

std::shared_ptr<TableStatistics> TableNode::create_statistics() const {
  return StorageManager::get().get_table(_table_name)->get_table_statistics();
}

}  // namespace opossum
