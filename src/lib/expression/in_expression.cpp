#include "in_expression.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace hyrise {

InExpression::InExpression(const PredicateCondition init_predicate_condition,
                           const std::shared_ptr<AbstractExpression>& value,
                           const std::shared_ptr<AbstractExpression>& set)
    : AbstractPredicateExpression(init_predicate_condition, {value, set}) {
  DebugAssert(predicate_condition == PredicateCondition::In || predicate_condition == PredicateCondition::NotIn,
              "Expected either IN or NOT IN as PredicateCondition");
}

bool InExpression::is_negated() const {
  return predicate_condition == PredicateCondition::NotIn;
}

const std::shared_ptr<AbstractExpression>& InExpression::value() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& InExpression::set() const {
  return arguments[1];
}

std::shared_ptr<AbstractExpression> InExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<InExpression>(predicate_condition, value()->deep_copy(copied_ops),
                                        set()->deep_copy(copied_ops));
}

std::string InExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << _enclose_argument(*value(), mode) << " ";
  stream << predicate_condition << " ";
  stream << set()->description(mode);
  return stream.str();
}

}  // namespace hyrise
