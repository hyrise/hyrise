#include "lqp_select_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

LQPSelectExpression::LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp): AbstractExpression(ExpressionType::Select, {}), lqp(lqp) {

}

std::shared_ptr<AbstractExpression> LQPSelectExpression::deep_copy() const {
  return std::make_shared<LQPSelectExpression>(lqp->deep_copy());
}

std::string LQPSelectExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

bool LQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  return !lqp->find_first_subplan_mismatch(static_cast<const LQPSelectExpression&>(expression).lqp);
}

}  // namespace opossum
