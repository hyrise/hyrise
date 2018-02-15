#include "constant_calculation_rule.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "resolve_type.hpp"
#include "utils/arithmetic_operator_expression.hpp"

namespace opossum {

std::string ConstantCalculationRule::name() const { return "Constant Calculation Rule"; }

bool ConstantCalculationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Projection) {
    return _apply_to_children(node);
  }

  auto projection_node = std::static_pointer_cast<ProjectionNode>(node);

  const auto column_expressions = projection_node->column_expressions();
  for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); column_id++) {
    const auto& expression = column_expressions[column_id];
    if (!expression->is_arithmetic_operator()) {
      continue;
    }

    // If the expression contains operands with different types, or one of the operands is a column reference,
    // it can't be resolved in this rule, and will be handled by the Projection later.
    const auto expression_type = _get_type_of_expression(expression);
    if (expression_type == std::nullopt) {
      continue;
    }

    auto value = std::optional<AllTypeVariant>{};

    if (*expression_type == DataType::Null) {
      value = NULL_VALUE;
    } else {
      resolve_data_type(*expression_type, [&](auto type) { value = _calculate_expression(type, expression); });
    }

    // If the value is std::nullopt, then the expression could not be resolved at this point,
    // e.g. because it is not an arithmetic operator.
    if (value != std::nullopt &&
        _replace_expression_in_parents(node, node->output_column_references()[column_id], *value)) {
      // If we successfully replaced the occurrences of the expression in the parent tree,
      // we can remove the column here.
      _remove_column_from_projection(projection_node, column_id);
    }
  }

  return _apply_to_children(node);
}

bool ConstantCalculationRule::_replace_expression_in_parents(const std::shared_ptr<AbstractLQPNode>& node,
                                                             const LQPColumnReference& expression_column,
                                                             const AllTypeVariant& value) {
  auto parent_tree_changed = false;
  for (auto parent : node->parents()) {
    if (parent->type() != LQPNodeType::Predicate) {
      parent_tree_changed |= _replace_expression_in_parents(parent, expression_column, value);
      continue;
    }

    auto predicate_node = std::static_pointer_cast<PredicateNode>(parent);

    // We look for a PredicateNode which has an LQPColumnReference as value,
    // referring to the column which contains the Expression we resolved before.
    if (is_lqp_column_reference(predicate_node->value()) &&
        boost::get<LQPColumnReference>(predicate_node->value()) == expression_column) {
      // We replace the LQPColumnReference with the actual result of the Expression it was referring to.
      auto new_predicate_node = std::make_shared<PredicateNode>(predicate_node->column_reference(),
                                                                predicate_node->predicate_condition(), value);
      predicate_node->replace_with(new_predicate_node);
      parent_tree_changed = true;
    }

    parent_tree_changed |= _replace_expression_in_parents(parent, expression_column, value);
  }
  return parent_tree_changed;
}

void ConstantCalculationRule::_remove_column_from_projection(const std::shared_ptr<ProjectionNode>& node,
                                                             ColumnID column_id) {
  auto column_expressions = node->column_expressions();
  column_expressions.erase(column_expressions.begin() + column_id);

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
  node->replace_with(projection_node);
}

std::optional<DataType> ConstantCalculationRule::_get_type_of_expression(
    const std::shared_ptr<LQPExpression>& expression) const {
  if (expression->type() == ExpressionType::Literal) {
    return data_type_from_all_type_variant(expression->value());
  }

  if (!expression->is_arithmetic_operator()) {
    return std::nullopt;
  }

  const auto type_left = _get_type_of_expression(expression->left_child());
  const auto type_right = _get_type_of_expression(expression->right_child());

  if (type_left == DataType::Null) return type_right;
  if (type_right == DataType::Null) return type_left;
  if (type_left != type_right) return std::nullopt;

  return type_left;
}

template <typename T>
std::optional<AllTypeVariant> ConstantCalculationRule::_calculate_expression(
    boost::hana::basic_type<T> type, const std::shared_ptr<LQPExpression>& expression) const {
  if (expression->type() == ExpressionType::Literal) {
    return expression->value();
  }

  if (!expression->is_arithmetic_operator()) {
    return std::nullopt;
  }

  const auto& arithmetic_operator_function = function_for_arithmetic_expression<T>(expression->type());

  const auto& left_expr = _calculate_expression(type, expression->left_child());
  const auto& right_expr = _calculate_expression(type, expression->right_child());

  if (variant_is_null(*left_expr) || variant_is_null(*right_expr)) {
    return NULL_VALUE;
  }

  return AllTypeVariant(arithmetic_operator_function(boost::get<T>(*left_expr), boost::get<T>(*right_expr)));
}

}  // namespace opossum
