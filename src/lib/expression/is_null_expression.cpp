#include "is_null_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

IsNullExpression::IsNullExpression(const PredicateCondition predicate_condition,
                                   const std::shared_ptr<AbstractExpression>& operand)
    : AbstractPredicateExpression(predicate_condition, {operand}) {
  Assert(predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull,
         "IsNullExpression only supports PredicateCondition::IsNull and PredicateCondition::IsNotNull");
}

const std::shared_ptr<AbstractExpression>& IsNullExpression::operand() const { return arguments[0]; }

std::shared_ptr<AbstractExpression> IsNullExpression::deep_copy() const {
  return std::make_shared<IsNullExpression>(predicate_condition, operand()->deep_copy());
}

std::string IsNullExpression::as_column_name() const {
  std::stringstream stream;

  if (predicate_condition == PredicateCondition::IsNull) {
    stream << _enclose_argument_as_column_name(*operand()) << " IS NULL";
  } else {
    stream << _enclose_argument_as_column_name(*operand()) << " IS NOT NULL";
  }

  return stream.str();
}

ExpressionPrecedence IsNullExpression::_precedence() const { return ExpressionPrecedence::UnaryPredicate; }

bool IsNullExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // IS NULL always returns a boolean value, never NULL
  return false;
}

}  // namespace opossum
