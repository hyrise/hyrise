#include "constant_calculation_rule.hpp"

#include <functional>
#include <map>
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
  auto column_reference_to_value_map = std::map<LQPColumnReference, AllTypeVariant>();
  _calculate_expressions_in_tree(node, column_reference_to_value_map);
  auto tree_changed = _replace_column_references_in_tree(node, column_reference_to_value_map);
  tree_changed |= _remove_columns_from_projections(node, column_reference_to_value_map);
  return tree_changed;
}

void ConstantCalculationRule::_calculate_expressions_in_tree(const std::shared_ptr<AbstractLQPNode>& node, std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  if (node->type() == LQPNodeType::Projection) {
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

      if (value != std::nullopt){
        column_reference_to_value_map.emplace(projection_node->output_column_references()[column_id], *value);
      }
    }
  }

  if (node->left_child()) {
    _calculate_expressions_in_tree(node->left_child(), column_reference_to_value_map);
  }
  if (node->right_child()) {
    _calculate_expressions_in_tree(node->right_child(), column_reference_to_value_map);
  }
}

bool ConstantCalculationRule::_replace_column_references_in_tree(const std::shared_ptr<AbstractLQPNode>& node, const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  auto tree_changed = false;
  if (node->type() == LQPNodeType::Predicate) {
    auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

    // We look for a PredicateNode which has an LQPColumnReference as value,
    // referring to the column which contains the Expression we resolved before.
    if (is_lqp_column_reference(predicate_node->value())) {
      const auto column_reference = boost::get<LQPColumnReference>(predicate_node->value());
      if (column_reference_to_value_map.find(column_reference) != column_reference_to_value_map.end()) {
        // We replace the LQPColumnReference with the actual result of the Expression it was referring to.
        auto new_predicate_node = std::make_shared<PredicateNode>(predicate_node->column_reference(), predicate_node->predicate_condition(), column_reference_to_value_map.at(column_reference));
        predicate_node->replace_with(new_predicate_node);
        tree_changed = true;
      }
    }
  }

  if (node->left_child()) {
    tree_changed |= _replace_column_references_in_tree(node->left_child(), column_reference_to_value_map);
  }
  if (node->right_child()) {
    tree_changed |= _replace_column_references_in_tree(node->right_child(), column_reference_to_value_map);
  }

  return tree_changed;
}

bool ConstantCalculationRule::_remove_columns_from_projections(const std::shared_ptr<AbstractLQPNode>& node, const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  auto tree_changed = false;
  if (node->type() == LQPNodeType::Projection) {
    auto projection_node = std::static_pointer_cast<ProjectionNode>(node);
    auto new_column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
    auto column_id = ColumnID{0};
    for (const auto column_reference : projection_node->output_column_references()) {
      if (column_reference_to_value_map.find(column_reference) == column_reference_to_value_map.end()) {
        new_column_expressions.push_back(projection_node->column_expressions()[column_id]);
      }
      column_id++;
    }

    auto new_projection_node = std::make_shared<ProjectionNode>(new_column_expressions);
    projection_node->replace_with(new_projection_node);
    tree_changed = true;
  }

  if (node->left_child()) {
    tree_changed |= _remove_columns_from_projections(node->left_child(), column_reference_to_value_map);
  }
  if (node->right_child()) {
    tree_changed |= _remove_columns_from_projections(node->right_child(), column_reference_to_value_map);
  }

  return tree_changed;
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
