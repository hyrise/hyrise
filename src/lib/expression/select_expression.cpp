#include "select_expression.hpp"

#include <sstream>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

SelectExpression::SelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp): AbstractExpression(ExpressionType::Select, {}), lqp(lqp) {

}

std::shared_ptr<AbstractExpression> SelectExpression::deep_copy() const {
  return std::make_shared<SelectExpression>(lqp->deep_copy());
}

std::string SelectExpression::description() const {
  std::stringstream stream;


  Fail("Todo");
  return stream.str();
}

bool SelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  return !lqp->find_first_subplan_mismatch(static_cast<const SelectExpression&>(expression).lqp);
}

}  // namespace opossum
