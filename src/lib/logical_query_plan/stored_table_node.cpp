#include "stored_table_node.hpp"

#include "expression/lqp_column_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"

namespace opossum {

StoredTableNode::StoredTableNode(const std::string& table_name):
  AbstractLQPNode(LQPNodeType::StoredTable), table_name(table_name) {
}

bool StoredTableNode::shallow_equals(const AbstractLQPNode& rhs) const {
  return false;
}

const std::vector<std::shared_ptr<AbstractExpression>>& StoredTableNode::output_column_expressions() const {
  // Need to initialize the expressions lazily because they will have a weak_ptr to this node and we can't obtain that
  // in the constructor
  if (!_expressions) {
    const auto table = StorageManager::get().get_table(table_name);

    _expressions.emplace(table->column_count());
    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      (*_expressions)[column_id] = std::make_shared<LQPColumnExpression>(LQPColumnReference{shared_from_this(), column_id});
    }
  }

  return *_expressions;
}

std::shared_ptr<AbstractLQPNode> StoredTableNode::deep_copy() const {
  return std::make_shared<StoredTableNode>(table_name);
}

}  // namespace opossum

//#include "stored_table_node.hpp"
//
//#include <memory>
//#include <optional>
//#include <string>
//#include <vector>
//
//#include "optimizer/table_statistics.hpp"
//#include "storage/storage_manager.hpp"
//#include "storage/table.hpp"
//#include "types.hpp"
//
//namespace opossum {
//
//StoredTableNode::StoredTableNode(const std::string& table_name, const std::optional<std::string>& alias)
//    : AbstractLQPNode(LQPNodeType::StoredTable), _table_name(table_name) {
//  /**
//   * Initialize output information.
//   */
//  auto table = StorageManager::get().get_table(_table_name);
//  _output_column_names = table->column_names();
//
//  set_alias(alias);
//}
//
//std::shared_ptr<AbstractLQPNode> StoredTableNode::_deep_copy_impl(
//    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
//    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
//  return StoredTableNode::make(_table_name);
//}
//
//std::string StoredTableNode::description() const { return "[StoredTable] Name: '" + _table_name + "'"; }
//
//std::shared_ptr<const AbstractLQPNode> StoredTableNode::find_table_name_origin(const std::string& table_name) const {
//  if (_table_alias) {
//    return *_table_alias == table_name ? shared_from_this() : nullptr;
//  }
//
//  return table_name == _table_name ? shared_from_this() : nullptr;
//}
//
//const std::vector<std::string>& StoredTableNode::output_column_names() const { return _output_column_names; }
//
//std::shared_ptr<TableStatistics> StoredTableNode::derive_statistics_from(
//    const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
//  DebugAssert(!left_input && !right_input, "StoredTableNode must be leaf");
//  return StorageManager::get().get_table(_table_name)->table_statistics();
//}
//
//const std::string& StoredTableNode::table_name() const { return _table_name; }
//
//std::string StoredTableNode::get_verbose_column_name(ColumnID column_id) const {
//  if (_table_alias) {
//    return "(" + _table_name + " AS " + *_table_alias + ")." + output_column_names()[column_id];
//  }
//  return _table_name + "." + output_column_names()[column_id];
//}
//
//bool StoredTableNode::shallow_equals(const AbstractLQPNode& rhs) const {
//  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
//  const auto& stored_table_node = static_cast<const StoredTableNode&>(rhs);
//
//  return _table_name == stored_table_node._table_name;
//}
//
//void StoredTableNode::_on_input_changed() { Fail("StoredTableNode cannot have inputs."); }
//
//std::optional<QualifiedColumnName> StoredTableNode::_resolve_local_table_name(
//    const QualifiedColumnName& qualified_column_name) const {
//  if (!qualified_column_name.table_name) {
//    return qualified_column_name;
//  }
//
//  if (_table_alias) {
//    if (*qualified_column_name.table_name != *_table_alias) {
//      return std::nullopt;
//    }
//  } else {
//    if (_table_name != *qualified_column_name.table_name) {
//      return std::nullopt;
//    }
//  }
//
//  auto reference_without_local_alias = qualified_column_name;
//  reference_without_local_alias.table_name = std::nullopt;
//  return reference_without_local_alias;
//}
//
//void StoredTableNode::set_excluded_chunk_ids(const std::vector<ChunkID>& chunks) { _excluded_chunk_ids = chunks; }
//
//const std::vector<ChunkID>& StoredTableNode::excluded_chunk_ids() const { return _excluded_chunk_ids; }
//
//}  // namespace opossum
