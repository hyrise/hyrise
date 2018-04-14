#include "lqp_select_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

LQPSelectExpression::LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp,
                                         const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions):
  AbstractExpression(ExpressionType::Select, referenced_external_expressions), lqp(lqp) {

}

std::shared_ptr<AbstractExpression> LQPSelectExpression::deep_copy() const {
  return std::make_shared<LQPSelectExpression>(lqp->deep_copy(), expressions_copy(referenced_external_expressions()));
}

std::string LQPSelectExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");
  return stream.str();
}

const std::vector<std::shared_ptr<AbstractExpression>>& LQPSelectExpression::referenced_external_expressions() const {
  return arguments;
}

bool LQPSelectExpression::_shallow_equals(const AbstractExpression& expression) const {
  return !lqp->find_first_subplan_mismatch(static_cast<const LQPSelectExpression&>(expression).lqp);
}

size_t LQPSelectExpression::_on_hash() const {
  return lqp->hash();
}

}  // namespace opossum
