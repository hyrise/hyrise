#include "abstract_predicate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

namespace opossum {

AbstractPredicateExpression::AbstractPredicateExpression(
    const PredicateCondition predicate_condition, const std::vector<std::shared_ptr<AbstractExpression>>& arguments)
    : AbstractExpression(ExpressionType::Predicate, arguments), predicate_condition(predicate_condition) {}

DataType AbstractPredicateExpression::data_type() const {
  // Should be Bool, but we don't have that.
  return DataType::Int;
}

bool AbstractPredicateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return predicate_condition == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition;
}

size_t AbstractPredicateExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(predicate_condition));
}

uint32_t AbstractPredicateExpression::_precedence() const {
  switch (predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
    case PredicateCondition::Between:
    case PredicateCondition::In:
    case PredicateCondition::Like:
    case PredicateCondition::NotLike:
      return 5;
    case PredicateCondition::IsNull:
    case PredicateCondition::IsNotNull:
      return 1;
  }
}
}  // namespace opossum
