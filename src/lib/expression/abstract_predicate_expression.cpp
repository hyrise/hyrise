#include "abstract_predicate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"
#include "expression/evaluation/expression_evaluator.hpp"

namespace opossum {

AbstractPredicateExpression::AbstractPredicateExpression(
    const PredicateCondition init_predicate_condition,
    const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments)
    : AbstractExpression(ExpressionType::Predicate, init_arguments), predicate_condition(init_predicate_condition) {}

DataType AbstractPredicateExpression::data_type() const { return ExpressionEvaluator::DataTypeBool; }

bool AbstractPredicateExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AbstractPredicateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return predicate_condition == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition;
}

size_t AbstractPredicateExpression::_shallow_hash() const {
  return boost::hash_value(static_cast<size_t>(predicate_condition));
}

}  // namespace opossum
