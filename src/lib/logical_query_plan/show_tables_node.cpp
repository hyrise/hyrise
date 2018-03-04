#include "show_tables_node.hpp"

#include <string>

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractLQPNode(LQPNodeType::ShowTables) {}

std::shared_ptr<AbstractLQPNode> ShowTablesNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return ShowTablesNode::make();
}

std::string ShowTablesNode::description() const { return "[ShowTables]"; }

const std::vector<std::string>& ShowTablesNode::output_column_names() const {
  static std::vector<std::string> output_column_names_dummy;
  return output_column_names_dummy;
}

bool ShowTablesNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  return true;
}

}  // namespace opossum
