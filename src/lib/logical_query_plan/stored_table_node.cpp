#include "stored_table_node.hpp"

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "optimizer/table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "types.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::StoredTable), _table_name(table_name) {
  /**
   * Initialize output information.
   */
  auto table = StorageManager::get().get_table(_table_name);
  _output_column_names = table->column_names();
}

std::shared_ptr<AbstractLQPNode> StoredTableNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<StoredTableNode>(_table_name);
}

std::string StoredTableNode::description() const { return "[StoredTable] Name: '" + _table_name + "'"; }

std::shared_ptr<const AbstractLQPNode> StoredTableNode::find_table_name_origin(const std::string& table_name) const {
  if (_table_alias) {
    return *_table_alias == table_name ? shared_from_this() : nullptr;
  }

  return table_name == _table_name ? shared_from_this() : nullptr;
}

const std::vector<std::string>& StoredTableNode::output_column_names() const { return _output_column_names; }

std::shared_ptr<TableStatistics> StoredTableNode::derive_statistics_from(
    const std::shared_ptr<AbstractLQPNode>& left_child, const std::shared_ptr<AbstractLQPNode>& right_child) const {
  DebugAssert(!left_child && !right_child, "StoredTableNode must be leaf");
  return StorageManager::get().get_table(_table_name)->table_statistics();
}

const std::string& StoredTableNode::table_name() const { return _table_name; }

std::string StoredTableNode::get_verbose_column_name(ColumnID column_id) const {
  if (_table_alias) {
    return "(" + _table_name + " AS " + *_table_alias + ")." + output_column_names()[column_id];
  }
  return _table_name + "." + output_column_names()[column_id];
}

void StoredTableNode::_on_child_changed() { Fail("StoredTableNode cannot have children."); }

std::optional<QualifiedColumnName> StoredTableNode::_resolve_local_table_name(
    const QualifiedColumnName& qualified_column_name) const {
  if (!qualified_column_name.table_name) {
    return qualified_column_name;
  }

  if (_table_alias) {
    if (*qualified_column_name.table_name != *_table_alias) {
      return std::nullopt;
    }
  } else {
    if (_table_name != *qualified_column_name.table_name) {
      return std::nullopt;
    }
  }

  auto reference_without_local_alias = qualified_column_name;
  reference_without_local_alias.table_name = std::nullopt;
  return reference_without_local_alias;
}

}  // namespace opossum
