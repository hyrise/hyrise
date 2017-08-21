#include "stored_table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name, const optional<std::string>& alias)
    : AbstractASTNode(ASTNodeType::StoredTable), _table_name(table_name), _alias(alias) {
  /**
   * Initialize output information.
   */
  auto table = StorageManager::get().get_table(_table_name);
  _output_column_names = table->column_names();

  for (uint16_t column_idx = 0u; column_idx < table->column_names().size(); column_idx++) {
    _output_column_ids.emplace_back(ColumnID{column_idx});
  }
}

std::string StoredTableNode::description() const { return "Table: " + _table_name; }

const std::vector<ColumnID>& StoredTableNode::output_column_ids() const { return _output_column_ids; }

const std::vector<std::string>& StoredTableNode::output_column_names() const { return _output_column_names; }

const std::shared_ptr<TableStatistics> StoredTableNode::_gather_statistics() const {
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

optional<ColumnID> StoredTableNode::find_column_id_for_column_identifier(
    const ColumnIdentifier& column_identifier) const {
  auto table = StorageManager::get().get_table(_table_name);
  return table->column_id_by_name(column_identifier.column_name);
}

bool StoredTableNode::manages_table(const std::string& table_name) const {
  return _alias == table_name || _table_name == table_name;
}

void StoredTableNode::_on_child_changed() { Fail("StoredTableNode cannot have children."); }

}  // namespace opossum
