#include "show_columns_node.hpp"

#include <string>

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::ShowColumns), _table_name(table_name) {}

std::string ShowColumnsNode::description() const { return "[ShowColumns] Table: '" + _table_name + "'"; }

const std::string& ShowColumnsNode::table_name() const { return _table_name; }

std::shared_ptr<AbstractLQPNode> ShowColumnsNode::_shallow_copy_impl(LQPNodeMapping& node_mapping) const {
  return ShowColumnsNode::make(_table_name);
}

bool ShowColumnsNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& show_columns_node_rhs = static_cast<const ShowColumnsNode&>(rhs);
  return _table_name == show_columns_node_rhs._table_name;
}

}  // namespace opossum
