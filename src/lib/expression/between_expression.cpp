#include "between_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace opossum {

PredicateCondition BetweenExpression::get_between_predicate_expression(bool left_inclusive, bool right_inclusive) {
  if (left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenInclusive;
  } else if (left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenUpperExclusive;
  } else if (!left_inclusive && right_inclusive) {
    return PredicateCondition::BetweenLowerExclusive;
  } else if (!left_inclusive && !right_inclusive) {
    return PredicateCondition::BetweenExclusive;
  }
  Fail("Unreachable Case");
}

// static (class) helper method to unify the mapping between predicate conditions and left inclusiveness
bool BetweenExpression::left_inclusive(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::BetweenInclusive || predicate_condition == PredicateCondition::BetweenUpperExclusive;
}

// static (class) helper method to unify the mapping between predicate conditions and right inclusiveness
bool BetweenExpression::right_inclusive(PredicateCondition predicate_condition) {
  return predicate_condition == PredicateCondition::BetweenInclusive || predicate_condition == PredicateCondition::BetweenLowerExclusive;
}

BetweenExpression::BetweenExpression(const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound,
                                     const PredicateCondition predicate_condition)
    : AbstractPredicateExpression(predicate_condition, {value, lower_bound, upper_bound}) {
  Assert(predicate_condition == PredicateCondition::BetweenInclusive ||
             predicate_condition == PredicateCondition::BetweenLowerExclusive ||
             predicate_condition == PredicateCondition::BetweenUpperExclusive ||
             predicate_condition == PredicateCondition::BetweenExclusive,
         "Predicate Condition not supported by Between Expression");
}

const std::shared_ptr<AbstractExpression>& BetweenExpression::value() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::lower_bound() const { return arguments[1]; }

const std::shared_ptr<AbstractExpression>& BetweenExpression::upper_bound() const { return arguments[2]; }

bool BetweenExpression::left_inclusive() const { return left_inclusive(predicate_condition); }

bool BetweenExpression::right_inclusive() const { return right_inclusive(predicate_condition); }

std::shared_ptr<AbstractExpression> BetweenExpression::deep_copy() const {
  return std::make_shared<BetweenExpression>(value()->deep_copy(), lower_bound()->deep_copy(),
                                             upper_bound()->deep_copy(), predicate_condition);
}

std::string BetweenExpression::as_column_name() const {
  std::stringstream stream;
  stream << _enclose_argument_as_column_name(*value()) << " BETWEEN "
         << _enclose_argument_as_column_name(*lower_bound()) << (left_inclusive() ? "" : " (exclusive)") << " AND "
         << _enclose_argument_as_column_name(*upper_bound()) << (right_inclusive() ? "" : " (exclusive)");
  return stream.str();
}

ExpressionPrecedence BetweenExpression::_precedence() const { return ExpressionPrecedence::BinaryTernaryPredicate; }

BetweenLowerExclusiveExpression::BetweenLowerExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : BetweenExpression(value, lower_bound, upper_bound, PredicateCondition::BetweenLowerExclusive) {}

BetweenUpperExclusiveExpression::BetweenUpperExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : BetweenExpression(value, lower_bound, upper_bound, PredicateCondition::BetweenUpperExclusive) {}

BetweenExclusiveExpression::BetweenExclusiveExpression(const std::shared_ptr<AbstractExpression>& value,
                                     const std::shared_ptr<AbstractExpression>& lower_bound,
                                     const std::shared_ptr<AbstractExpression>& upper_bound)
    : BetweenExpression(value, lower_bound, upper_bound, PredicateCondition::BetweenExclusive) {}

}  // namespace opossum
