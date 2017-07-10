#include "table_node.hpp"

#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

TableNode::TableNode(const std::string table_name) : AbstractAstNode(AstNodeType::Table), _table_name(table_name) {}

std::string TableNode::description() const { return "Table: " + _table_name; }

const std::vector<std::string>& TableNode::output_columns() const {
  // Cache call to StorageManager.
  if (_column_names.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    _output_columns = table->column_names();
  }

  return _output_columns;
}

const std::string& TableNode::table_name() const { return _table_name; }

}  // namespace opossum
