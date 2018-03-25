#include "predicate_expression.hpp"

#include <sstream>

namespace opossum {

PredicateExpression::PredicateExpression(const PredicateCondition predicate_condition,
                                     const std::shared_ptr<AbstractExpression>& left_operand,
                                     const std::shared_ptr<AbstractExpression>& right_operand):
AbstractExpression(ExpressionType::Predicate, {left_operand, right_operand}), predicate_condition(predicate_condition) {}

const std::shared_ptr<AbstractExpression>& PredicateExpression::left_operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& PredicateExpression::right_operand() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression> PredicateExpression::deep_copy() const {
  return std::make_shared<PredicateExpression>(predicate_condition, left_operand()->deep_copy(), left_operand()->deep_copy());
}

std::string PredicateExpression::description() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool PredicateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return predicate_condition == static_cast<const PredicateExpression&>(expression).predicate_condition;
}

}  // namespace opossum
