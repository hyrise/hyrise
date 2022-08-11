#include "is_null_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace hyrise {

IsNullExpression::IsNullExpression(const PredicateCondition init_predicate_condition,
                                   const std::shared_ptr<AbstractExpression>& operand)
    : AbstractPredicateExpression(init_predicate_condition, {operand}) {
  Assert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
         "IsNullExpression only supports PredicateCondition::IsNull and PredicateCondition::IsNotNull");
}

const std::shared_ptr<AbstractExpression>& IsNullExpression::operand() const {
  return arguments[0];
}

std::shared_ptr<AbstractExpression> IsNullExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<IsNullExpression>(predicate_condition, operand()->deep_copy(copied_ops));
}

std::string IsNullExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (predicate_condition == PredicateCondition::IsNull) {
    stream << _enclose_argument(*operand(), mode) << " IS NULL";
  } else {
    stream << _enclose_argument(*operand(), mode) << " IS NOT NULL";
  }

  return stream.str();
}

ExpressionPrecedence IsNullExpression::_precedence() const {
  return ExpressionPrecedence::UnaryPredicate;
}

bool IsNullExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // IS NULL always returns a boolean value, never NULL
  return false;
}

}  // namespace hyrise
