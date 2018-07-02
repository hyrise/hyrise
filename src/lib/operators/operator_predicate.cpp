#include "operator_predicate.hpp"

#include "expression/abstract_predicate_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::optional<OperatorPredicate> OperatorPredicate::from_expression(const AbstractExpression& expression,
                                                                    const AbstractLQPNode& node) {
  const auto* predicate = dynamic_cast<const AbstractPredicateExpression*>(&expression);
  if (!predicate) return std::nullopt;

  Assert(!predicate->arguments.empty(), "Expect PredicateExpression to have one or more arguments");

  const auto column_id = node.find_column_id(*predicate->arguments[0]);
  if (!column_id) return std::nullopt;

  const auto predicate_condition = predicate->predicate_condition;

  if (predicate_condition == PredicateCondition::IsNull || predicate_condition == PredicateCondition::IsNotNull) {
    return OperatorPredicate{*column_id, predicate_condition};
  }

  Assert(predicate->arguments.size() > 1, "Expect non-unary PredicateExpression to have two or more arguments");

  auto value = AllParameterVariant{};
  if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression.arguments[1]);
      value_expression) {
    value = value_expression->value;
  } else if (const auto value_column_id = node.find_column_id(*expression.arguments[1]); value_column_id) {
    value = *value_column_id;
  } else {
    return std::nullopt;
  }

  if (predicate_condition == PredicateCondition::Between) {
    Assert(predicate->arguments.size() == 3, "Expect ternary PredicateExpression to have three arguments");
    if (const auto value_expression = std::dynamic_pointer_cast<ValueExpression>(expression.arguments[2]);
        value_expression) {
      return OperatorPredicate{*column_id, predicate_condition, value, value_expression->value};
    } else {
      return std::nullopt;
    }
  } else {
    return OperatorPredicate{*column_id, predicate_condition, value};
  }
}

OperatorPredicate::OperatorPredicate(const ColumnID column_id, const PredicateCondition predicate_condition,
                                     const AllParameterVariant& value, const std::optional<AllParameterVariant>& value2)
    : column_id(column_id), predicate_condition(predicate_condition), value(value), value2(value2) {}

}  // namespace opossum
