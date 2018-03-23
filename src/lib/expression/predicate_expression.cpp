#include "predicate_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

PredicateExpression::PredicateExpression(const PredicateCondition predicate_condition, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Predicate), predicate_condition(predicate_condition), left_operand(left_operand), right_operand(right_operand) {}

bool PredicateExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& predicate_expression = static_cast<const PredicateExpression&>(expression);
  if (predicate_condition != predicate_expression.predicate_condition) return false;
  return deep_equals_expressions({left_operand, right_operand}, {predicate_expression.left_operand, predicate_expression.right_operand});
}

std::shared_ptr<AbstractExpression> PredicateExpression::deep_copy() const {
  return std::make_shared<PredicateExpression>(predicate_condition, left_operand->deep_copy(), right_operand->deep_copy());
}

std::shared_ptr<AbstractExpression> PredicateExpression::deep_resolve_column_expressions() {
  left_operand = left_operand->deep_resolve_column_expressions();
  right_operand = right_operand->deep_resolve_column_expressions();

  return shared_from_this();
}

}  // namespace opossum
