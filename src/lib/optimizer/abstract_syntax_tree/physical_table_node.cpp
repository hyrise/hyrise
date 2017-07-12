#include "physical_table_node.hpp"

#include <string>
#include <vector>

#include "storage/storage_manager.hpp"

namespace opossum {

PhysicalTableNode::PhysicalTableNode(const std::string table_name)
    : AbstractASTNode(ASTNodeType::PhysicalTable), _table_name(table_name) {}

std::string PhysicalTableNode::description() const { return "Table: " + _table_name; }

const std::vector<std::string>& PhysicalTableNode::output_column_names() const {
  // Cache call to StorageManager.
  if (_output_column_names.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    _output_column_names = table->column_names();
  }

  return _output_column_names;
}

const std::string& PhysicalTableNode::table_name() const { return _table_name; }

}  // namespace opossum
