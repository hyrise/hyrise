#include "binary_predicate_expression.hpp"

#include <sstream>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

BinaryPredicateExpression::BinaryPredicateExpression(const PredicateCondition init_predicate_condition,
                                                     const std::shared_ptr<AbstractExpression>& left_operand,
                                                     const std::shared_ptr<AbstractExpression>& right_operand)
    : AbstractPredicateExpression(init_predicate_condition, {left_operand, right_operand}) {
  if constexpr (HYRISE_DEBUG) {
    const auto valid_predicate_conditions = {PredicateCondition::Equals,      PredicateCondition::NotEquals,
                                             PredicateCondition::GreaterThan, PredicateCondition::GreaterThanEquals,
                                             PredicateCondition::LessThan,    PredicateCondition::LessThanEquals,
                                             PredicateCondition::Like,        PredicateCondition::NotLike};
    const auto* const it =
        std::find(valid_predicate_conditions.begin(), valid_predicate_conditions.end(), predicate_condition);
    Assert(it != valid_predicate_conditions.end(),
           "Specified PredicateCondition is not valid for a BinaryPredicateExpression");
  }
}

const std::shared_ptr<AbstractExpression>& BinaryPredicateExpression::left_operand() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& BinaryPredicateExpression::right_operand() const { return arguments[1]; }

std::shared_ptr<AbstractExpression> BinaryPredicateExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand()->deep_copy(copied_ops),
                                                     right_operand()->deep_copy(copied_ops));
}

std::string BinaryPredicateExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << _enclose_argument(*left_operand(), mode) << " ";
  stream << predicate_condition << " ";
  stream << _enclose_argument(*right_operand(), mode);

  return stream.str();
}

ExpressionPrecedence BinaryPredicateExpression::_precedence() const {
  return ExpressionPrecedence::BinaryTernaryPredicate;
}

}  // namespace opossum
