#include "show_columns_node.hpp"

#include <string>

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::ShowColumns), _table_name(table_name) {}

std::shared_ptr<AbstractLQPNode> ShowColumnsNode::_deep_copy_impl(
    const std::shared_ptr<AbstractLQPNode>& copied_left_input,
    const std::shared_ptr<AbstractLQPNode>& copied_right_input) const {
  return ShowColumnsNode::make(_table_name);
}

std::string ShowColumnsNode::description() const { return "[ShowColumns] Table: '" + _table_name + "'"; }

const std::string& ShowColumnsNode::table_name() const { return _table_name; }

const std::vector<std::string>& ShowColumnsNode::output_column_names() const {
  static std::vector<std::string> output_column_names_dummy;
  return output_column_names_dummy;
}

bool ShowColumnsNode::shallow_equals(const AbstractLQPNode& rhs) const {
  Assert(rhs.type() == type(), "Can only compare nodes of the same type()");
  const auto& show_columns_node = dynamic_cast<const ShowColumnsNode&>(rhs);
  return _table_name == show_columns_node._table_name;
}

}  // namespace opossum
