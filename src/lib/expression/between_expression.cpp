#include "between_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace hyrise {

BetweenExpression::BetweenExpression(const PredicateCondition init_predicate_condition,
                                     const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : AbstractPredicateExpression(init_predicate_condition, {value, lower_bound, upper_bound}) {
  Assert(is_between_predicate_condition(predicate_condition),
         "Predicate Condition not supported by Between Expression");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::value() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::lower_bound() const {
  return arguments[1];
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::upper_bound() const {
  return arguments[2];
}

std::shared_ptr<AbstractExpression> BetweenExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<BetweenExpression>(predicate_condition, value()->deep_copy(copied_ops),
                                             lower_bound()->deep_copy(copied_ops),
                                             upper_bound()->deep_copy(copied_ops));
}

std::string BetweenExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << _enclose_argument(*value(), mode) << " " << predicate_condition << " "
         << _enclose_argument(*lower_bound(), mode) << " AND " << _enclose_argument(*upper_bound(), mode);
  return stream.str();
}

ExpressionPrecedence BetweenExpression::_precedence() const {
  return ExpressionPrecedence::BinaryTernaryPredicate;
}

}  // namespace hyrise
