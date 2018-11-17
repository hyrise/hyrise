#include "show_columns_node.hpp"

#include <string>

#include "expression/expression_functional.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

ShowColumnsNode::ShowColumnsNode(const std::string& table_name)
    : AbstractLQPNode(LQPNodeType::ShowColumns), table_name(table_name) {}

std::string ShowColumnsNode::description() const { return "[ShowColumns] Table: '" + table_name + "'"; }

const std::vector<std::shared_ptr<AbstractExpression>>& ShowColumnsNode::column_expressions() const {
  if (!_column_expressions) {
    _column_expressions.emplace();
    // column_name, column_type, is_nullable
    _column_expressions->emplace_back(lqp_column_(LQPColumnReference{shared_from_this(), ColumnID{0}}));
    _column_expressions->emplace_back(lqp_column_(LQPColumnReference{shared_from_this(), ColumnID{1}}));
    _column_expressions->emplace_back(lqp_column_(LQPColumnReference{shared_from_this(), ColumnID{2}}));
  }

  return *_column_expressions;
}

std::shared_ptr<AbstractLQPNode> ShowColumnsNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ShowColumnsNode::make(table_name);
}

bool ShowColumnsNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& show_columns_node_rhs = static_cast<const ShowColumnsNode&>(rhs);
  return table_name == show_columns_node_rhs.table_name;
}

}  // namespace opossum
