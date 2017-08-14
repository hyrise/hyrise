#include "stored_table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name)
    : AbstractASTNode(ASTNodeType::StoredTable), _table_name(table_name) {}

std::string StoredTableNode::description() const { return "Table: " + _table_name; }

std::vector<ColumnID> StoredTableNode::output_column_ids() const {
  // Cache call to StorageManager.
  if (_output_column_ids.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    // TODO(mp): fix
//    _output_column_ids = table->column_names();
  }

  return _output_column_ids;
}

const std::shared_ptr<TableStatistics> StoredTableNode::_gather_statistics() const {
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

bool StoredTableNode::find_column_id_for_column_name(std::string & column_name, ColumnID &column_id) {
  auto table = StorageManager::get().get_table(_table_name);
  column_id = table->column_id_by_name(column_name);
  return true;
}

}  // namespace opossum
