#include "show_columns_node.hpp"

#include <string>

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::ShowColumns), _table_name(table_name) {}

std::shared_ptr<AbstractLQPNode> ShowColumnsNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<ShowColumnsNode>(_table_name);
}

std::string ShowColumnsNode::description() const { return "[ShowColumns] Table: '" + _table_name + "'"; }

const std::string& ShowColumnsNode::table_name() const { return _table_name; }

}  // namespace opossum
