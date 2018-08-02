#include "constant_calculation_rule.hpp"

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_utils.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "resolve_type.hpp"

namespace opossum {

std::string ConstantCalculationRule::name() const { return "Constant Calculation Rule"; }

bool ConstantCalculationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // We can't prune Aggregate arguments, because the operator doesn't support, e.g., `MIN(1)`, whereas it supports
  // `MIN(2-1)`, since `2-1` is a column.
  if (node->type == LQPNodeType::Aggregate) return _apply_to_inputs(node);

  for (auto& expression : node->node_expressions()) {
    _prune_expression(expression);
  }

  return _apply_to_inputs(node);
}

void ConstantCalculationRule::_prune_expression(std::shared_ptr<AbstractExpression>& expression) const {
  for (auto& argument : expression->arguments) {
    _prune_expression(argument);
  }

  if (expression->arguments.empty()) return;

  // Only prune a whitelisted selection of ExpressionTypes, because we can't, e.g., prune List of literals.
  if (expression->type != ExpressionType::Predicate && expression->type != ExpressionType::Arithmetic &&
      expression->type != ExpressionType::Logical) {
    return;
  }

  const auto all_arguments_are_values =
      std::all_of(expression->arguments.begin(), expression->arguments.end(),
                  [&](const auto& argument) { return argument->type == ExpressionType::Value; });

  if (!all_arguments_are_values) return;

  resolve_data_type(expression->data_type(), [&](const auto data_type_t) {
    using ExpressionDataType = typename decltype(data_type_t)::type;
    const auto result = ExpressionEvaluator{}.evaluate_expression_to_result<ExpressionDataType>(*expression);
    Assert(result->is_literal(), "Expected Literal");

    if (result->is_null(0)) {
      expression = std::make_shared<ValueExpression>(NullValue{});
    } else {
      expression = std::make_shared<ValueExpression>(result->value(0));
    }
  });
}

}  // namespace opossum
