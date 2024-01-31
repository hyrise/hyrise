#include "in_expression.hpp"

#include <sstream>

#include "utils/assert.hpp"

namespace hyrise {

InExpression::InExpression(const PredicateCondition init_predicate_condition,
                           const std::shared_ptr<AbstractExpression>& operand,
                           const std::shared_ptr<AbstractExpression>& set)
    : AbstractPredicateExpression(init_predicate_condition, {operand, set}) {
  DebugAssert(predicate_condition == PredicateCondition::In || predicate_condition == PredicateCondition::NotIn,
              "Expected either IN or NOT IN as PredicateCondition");
}

bool InExpression::is_negated() const {
  return predicate_condition == PredicateCondition::NotIn;
}

const std::shared_ptr<AbstractExpression>& InExpression::operand() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& InExpression::set() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression> InExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<InExpression>(predicate_condition, operand()->deep_copy(copied_ops),
                                        set()->deep_copy(copied_ops));
}

std::string InExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};
  stream << _enclose_argument(*operand(), mode) << " ";
  stream << predicate_condition << " ";
  stream << set()->description(mode);
  return stream.str();
}

}  // namespace hyrise
