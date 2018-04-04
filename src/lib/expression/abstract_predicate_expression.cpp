#include "abstract_predicate_expression.hpp"

#include <sstream>

namespace opossum {

AbstractPredicateExpression::AbstractPredicateExpression(const PredicateCondition predicate_condition, const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
AbstractExpression(ExpressionType::Predicate, arguments), predicate_condition(predicate_condition) {}

bool AbstractPredicateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return predicate_condition == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition;
}

}  // namespace opossum
