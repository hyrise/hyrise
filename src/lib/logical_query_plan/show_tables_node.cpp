#include "show_tables_node.hpp"

#include <string>

#include "expression/expression_functional.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

ShowTablesNode::ShowTablesNode() : AbstractLQPNode(LQPNodeType::ShowTables) {}

std::string ShowTablesNode::description() const { return "[ShowTables]"; }

std::shared_ptr<AbstractLQPNode> ShowTablesNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return ShowTablesNode::make();
}

const std::vector<std::shared_ptr<AbstractExpression>>& ShowTablesNode::column_expressions() const {
  if (!_column_expressions) {
    _column_expressions.emplace();
    _column_expressions->emplace_back(column_(LQPColumnReference{shared_from_this(), ColumnID{0}}));
  }

  return *_column_expressions;
}

bool ShowTablesNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  return true;
}

}  // namespace opossum
