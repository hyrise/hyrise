#include "abstract_predicate_expression.hpp"

#include <cstddef>
#include <functional>
#include <memory>
#include <vector>

#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

AbstractPredicateExpression::AbstractPredicateExpression(
    const PredicateCondition init_predicate_condition,
    const std::vector<std::shared_ptr<AbstractExpression>>& init_arguments)
    : AbstractExpression(ExpressionType::Predicate, init_arguments), predicate_condition(init_predicate_condition) {}

DataType AbstractPredicateExpression::data_type() const {
  return ExpressionEvaluator::DataTypeBool;
}

bool AbstractPredicateExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AbstractPredicateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return predicate_condition == static_cast<const AbstractPredicateExpression&>(expression).predicate_condition;
}

size_t AbstractPredicateExpression::_shallow_hash() const {
  return std::hash<PredicateCondition>{}(predicate_condition);
}

}  // namespace hyrise
