#include "limit_node.hpp"

#include <sstream>
#include <string>

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "utils/assert.hpp"

namespace opossum {

LimitNode::LimitNode(const std::shared_ptr<AbstractExpression>& num_rows_expression)
    : AbstractLQPNode(LQPNodeType::Limit, {num_rows_expression}) {}

std::string LimitNode::description() const {
  std::stringstream stream;
  stream << "[Limit] " << num_rows_expression()->as_column_name();
  return stream.str();
}

std::shared_ptr<AbstractExpression> LimitNode::num_rows_expression() const { return node_expressions[0]; }

std::shared_ptr<AbstractLQPNode> LimitNode::_on_shallow_copy(LQPNodeMapping& node_mapping) const {
  return LimitNode::make(expression_copy_and_adapt_to_different_lqp(*num_rows_expression(), node_mapping));
}

bool LimitNode::_on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const {
  const auto& limit_node = static_cast<const LimitNode&>(rhs);
  return expression_equal_to_expression_in_different_lqp(*num_rows_expression(), *limit_node.num_rows_expression(),
                                                         node_mapping);
}

}  // namespace opossum
