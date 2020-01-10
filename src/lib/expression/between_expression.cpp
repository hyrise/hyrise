#include "between_expression.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

BetweenExpression::BetweenExpression(const PredicateCondition predicate_condition,
                                     const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : AbstractPredicateExpression(predicate_condition, {value, lower_bound, upper_bound}) {
  Assert(is_between_predicate_condition(predicate_condition),
         "Predicate Condition not supported by Between Expression");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::value() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::lower_bound() const { return arguments[1]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::upper_bound() const { return arguments[2]; }

std::shared_ptr<AbstractExpression> BetweenExpression::deep_copy() const {
  return std::make_shared<BetweenExpression>(predicate_condition, value()->deep_copy(), lower_bound()->deep_copy(),
                                             upper_bound()->deep_copy());
}

std::string BetweenExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << _enclose_argument(*value(), mode) << " " << predicate_condition << " "
         << _enclose_argument(*lower_bound(), mode) << " AND " << _enclose_argument(*upper_bound(), mode);
  return stream.str();
}

ExpressionPrecedence BetweenExpression::_precedence() const { return ExpressionPrecedence::BinaryTernaryPredicate; }

}  // namespace opossum
