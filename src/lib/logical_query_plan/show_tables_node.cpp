#include "show_tables_node.hpp"

#include <string>

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractLQPNode(LQPNodeType::ShowTables) {}

std::shared_ptr<AbstractLQPNode> ShowTablesNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_child,
    const std::shared_ptr<AbstractLQPNode>& copied_right_child) const {
  return std::make_shared<ShowTablesNode>();
}

std::string ShowTablesNode::description() const { return "[ShowTables]"; }

}  // namespace opossum
