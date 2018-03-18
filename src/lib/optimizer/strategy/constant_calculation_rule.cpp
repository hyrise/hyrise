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
  _calculate_expressions_in_lqp(node, column_reference_to_value_map);
  auto lqp_changed = _replace_column_references_in_lqp(node, column_reference_to_value_map);
  lqp_changed |= _remove_columns_from_projections(node, column_reference_to_value_map);
  return lqp_changed;
}

void ConstantCalculationRule::_calculate_expressions_in_lqp(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  if (node->left_input()) {
    _calculate_expressions_in_lqp(node->left_input(), column_reference_to_value_map);
  }
  if (node->right_input()) {
    _calculate_expressions_in_lqp(node->right_input(), column_reference_to_value_map);
  }

  if (node->type() != LQPNodeType::Projection) {
    return;
  }

  auto projection_node = std::static_pointer_cast<ProjectionNode>(node);

  const auto column_expressions = projection_node->column_expressions();
  for (auto column_id = ColumnID{0}; column_id < column_expressions.size(); column_id++) {
    const auto& expression = column_expressions[column_id];

    if (expression->type() == ExpressionType::Literal) {
      continue;
    }

    // If the expression contains operands with different types, or one of the operands is a column reference,
    // it can't be resolved in this rule, and will be handled by the Projection later.
    const auto resolved_expression_type = _get_type_of_expression(expression);
    if (resolved_expression_type == std::nullopt) {
      continue;
    }

    auto value = std::optional<AllTypeVariant>{};

    if (*resolved_expression_type == DataType::Null) {
      value = NULL_VALUE;
    } else {
      resolve_data_type(*resolved_expression_type, [&](auto type) { value = _calculate_expression(type, expression); });
    }

    if (value != std::nullopt) {
      column_reference_to_value_map.emplace(projection_node->output_column_references()[column_id], *value);
    }
  }
}

bool ConstantCalculationRule::_replace_column_references_in_lqp(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  auto lqp_changed = false;

  if (node->left_input()) {
    lqp_changed |= _replace_column_references_in_lqp(node->left_input(), column_reference_to_value_map);
  }
  if (node->right_input()) {
    lqp_changed |= _replace_column_references_in_lqp(node->right_input(), column_reference_to_value_map);
  }

  if (node->type() == LQPNodeType::Predicate) {
    auto predicate_node = std::static_pointer_cast<PredicateNode>(node);

    // We look for a PredicateNode which has an LQPColumnReference as value,
    // referring to the column which contains the Expression we resolved before.
    if (is_lqp_column_reference(predicate_node->value())) {
      const auto column_reference = boost::get<LQPColumnReference>(predicate_node->value());
      auto iter = column_reference_to_value_map.find(column_reference);
      if (iter != column_reference_to_value_map.end()) {
        // We replace the LQPColumnReference with the actual result of the Expression it was referring to.
        auto new_predicate_node =
            std::make_shared<PredicateNode>(predicate_node->column_reference(), predicate_node->predicate_condition(),
                                            iter->second, predicate_node->value2());
        predicate_node->replace_with(new_predicate_node);
        lqp_changed = true;
      }
    }
  } else if (node->type() == LQPNodeType::Projection) {
    auto projection_node = std::static_pointer_cast<ProjectionNode>(node);
    auto new_column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
    auto projection_node_changed = false;

    for (const auto expression : projection_node->column_expressions()) {
      auto new_expression = _replace_column_references_in_expression(expression, column_reference_to_value_map);
      new_column_expressions.push_back(new_expression);

      if (new_expression.get() != expression.get()) {
        projection_node_changed = true;
      }
    }

    if (projection_node_changed) {
      auto new_projection_node = std::make_shared<ProjectionNode>(new_column_expressions);
      projection_node->replace_with(new_projection_node);
      lqp_changed = true;
    }
  }

  return lqp_changed;
}

std::shared_ptr<LQPExpression> ConstantCalculationRule::_replace_column_references_in_expression(
    const std::shared_ptr<LQPExpression>& expression,
    const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  if (expression->type() == ExpressionType::Column) {
    auto iter = column_reference_to_value_map.find(expression->column_reference());
    if (iter == column_reference_to_value_map.end()) {
      return expression;
    }

    return LQPExpression::create_literal(iter->second);
  }

  if (expression->is_unary_operator()) {
    auto input = _replace_column_references_in_expression(expression->left_child(), column_reference_to_value_map);
    return LQPExpression::create_unary_operator(expression->type(), input, expression->alias());
  }

  if (expression->is_binary_operator()) {
    auto left_child = expression->left_child();
    auto right_child = expression->right_child();
    auto new_left_child = _replace_column_references_in_expression(left_child, column_reference_to_value_map);
    auto new_right_child = _replace_column_references_in_expression(right_child, column_reference_to_value_map);

    if (left_child == new_left_child && right_child == new_right_child) {
      return expression;
    }

    return LQPExpression::create_binary_operator(expression->type(), new_left_child, new_right_child,
                                                 expression->alias());
  }

  return expression;
}

bool ConstantCalculationRule::_remove_columns_from_projections(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<LQPColumnReference, AllTypeVariant>& column_reference_to_value_map) {
  auto lqp_changed = false;

  if (node->left_input()) {
    lqp_changed |= _remove_columns_from_projections(node->left_input(), column_reference_to_value_map);
  }
  if (node->right_input()) {
    lqp_changed |= _remove_columns_from_projections(node->right_input(), column_reference_to_value_map);
  }

  if (node->type() == LQPNodeType::Projection) {
    for (const auto& output_node : node->outputs()) {
      // Don't remove any columns if this is the result node
      if (output_node->type() == LQPNodeType::Root) {
        return lqp_changed;
      }
    }

    auto projection_node = std::static_pointer_cast<ProjectionNode>(node);
    auto new_column_expressions = std::vector<std::shared_ptr<LQPExpression>>();
    auto column_id = ColumnID{0};
    auto replace_or_remove_projection_node = false;
    for (const auto column_reference : projection_node->output_column_references()) {
      if (column_reference_to_value_map.find(column_reference) == column_reference_to_value_map.end()) {
        new_column_expressions.push_back(projection_node->column_expressions()[column_id]);
      } else {
        replace_or_remove_projection_node = true;
      }
      column_id++;
    }

    if (replace_or_remove_projection_node) {
      if (new_column_expressions.size() > 0) {
        auto new_projection_node = std::make_shared<ProjectionNode>(new_column_expressions);
        projection_node->replace_with(new_projection_node);
      } else {
        projection_node->remove_from_tree();
      }
      lqp_changed = true;
    }
  }

  return lqp_changed;
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
