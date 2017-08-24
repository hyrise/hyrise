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

  _output_column_ids.resize(_output_column_names.size(), INVALID_COLUMN_ID);
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

  if (column_identifier.table_name && !manages_table(*column_identifier.table_name)) {
    return nullopt;
  }

  auto &columns = output_column_names();
  auto iter = std::find(columns.begin(), columns.end(), column_identifier.column_name);

  if (iter == columns.end()){
    return nullopt;
  }

  auto idx = std::distance(columns.begin(), iter);
  return ColumnID{static_cast<ColumnID::base_type>(idx)};
}

bool StoredTableNode::manages_table(const std::string& table_name) const {
  return _alias == table_name || _table_name == table_name;
}

void StoredTableNode::_on_child_changed() { Fail("StoredTableNode cannot have children."); }

}  // namespace opossum
