#include "between_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

BetweenExpression::BetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound, bool left_inclusive,
                                     bool right_inclusive)
    : AbstractPredicateExpression(PredicateCondition::BetweenInclusive, {value, lower_bound, upper_bound}),
      _left_inclusive{left_inclusive},
      _right_inclusive{right_inclusive} {}

const std::shared_ptr<AbstractExpression>& BetweenExpression::value() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::lower_bound() const { return arguments[1]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::upper_bound() const { return arguments[2]; }

bool BetweenExpression::left_inclusive() const { return _left_inclusive; }

bool BetweenExpression::right_inclusive() const { return _right_inclusive; }

std::shared_ptr<AbstractExpression> BetweenExpression::deep_copy() const {
  return std::make_shared<BetweenExpression>(value()->deep_copy(), lower_bound()->deep_copy(),
                                             upper_bound()->deep_copy(), _left_inclusive, _right_inclusive);
}

std::string BetweenExpression::as_column_name() const {
  std::stringstream stream;
  stream << _enclose_argument_as_column_name(*value()) << " BETWEEN "
         << _enclose_argument_as_column_name(*lower_bound()) << (_left_inclusive ? "" : " (exclusive)") << " AND "
         << _enclose_argument_as_column_name(*upper_bound()) << (_right_inclusive ? "" : " (exclusive)");
  return stream.str();
}

ExpressionPrecedence BetweenExpression::_precedence() const { return ExpressionPrecedence::BinaryTernaryPredicate; }

}  // namespace opossum
