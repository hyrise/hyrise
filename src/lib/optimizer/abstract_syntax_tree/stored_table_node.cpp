#include "stored_table_node.hpp"

#include <memory>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name, optional<std::string> alias)
    : AbstractASTNode(ASTNodeType::StoredTable), _table_name(table_name), _alias(alias) {}

std::string StoredTableNode::description() const { return "Table: " + _table_name; }

const std::vector<ColumnID> StoredTableNode::output_column_ids() const {
  // Cache call to StorageManager.
  if (_output_column_ids.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    for (size_t i = 0; i < table->column_names().size(); i++) {
      _output_column_ids.emplace_back(ColumnID{i});
    }
  }

  return _output_column_ids;
}

const std::vector<std::string> StoredTableNode::output_column_names() const {
  // Cache call to StorageManager.
  if (_output_column_names.empty()) {
    auto table = StorageManager::get().get_table(_table_name);
    _output_column_names = table->column_names();
  }

  return _output_column_names;
}

const std::shared_ptr<TableStatistics> StoredTableNode::_gather_statistics() const {
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

const optional<ColumnID> StoredTableNode::find_column_id_for_column_identifier(
    ColumnIdentifier& column_identifier) const {
  auto table = StorageManager::get().get_table(_table_name);
  return table->column_id_by_name(column_identifier.column_name);
}

const std::string StoredTableNode::table_identifier() const { return _alias ? *_alias : _table_name; }

}  // namespace opossum
