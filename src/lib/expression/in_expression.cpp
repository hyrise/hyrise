#include "in_expression.hpp"

#include <sstream>

#include "constant_mappings.hpp"
#include "utils/assert.hpp"

namespace opossum {

InExpression::InExpression(const PredicateCondition predicate_condition,
                           const std::shared_ptr<AbstractExpression>& value,
                           const std::shared_ptr<AbstractExpression>& set)
    : AbstractPredicateExpression(predicate_condition, {value, set}) {
  DebugAssert(predicate_condition == PredicateCondition::In || predicate_condition == PredicateCondition::NotIn,
              "Expected either IN or NOT IN as PredicateCondition");
}

bool InExpression::is_negated() const { return predicate_condition == PredicateCondition::NotIn; }

const std::shared_ptr<AbstractExpression>& InExpression::value() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& InExpression::set() const { return arguments[1]; }

std::shared_ptr<AbstractExpression> InExpression::deep_copy() const {
  return std::make_shared<InExpression>(predicate_condition, value()->deep_copy(), set()->deep_copy());
}

std::string InExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << _enclose_argument(*value(), mode) << " ";
  stream << predicate_condition << " ";
  stream << set()->description(mode);
  return stream.str();
}

}  // namespace opossum
