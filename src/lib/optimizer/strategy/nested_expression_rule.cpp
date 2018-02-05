#include "nested_expression_rule.hpp"

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

std::string NestedExpressionRule::name() const { return "Nested Expression Rule"; }

bool NestedExpressionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Projection) {
    return _apply_to_children(node);
  }

  auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(node);

  auto column_id = ColumnID{0};
  for (auto expression : projection_node->column_expressions()) {
    if (expression->is_arithmetic_operator()) {
      const auto expression_type = _get_type_of_expression(expression);
      if (expression_type != std::nullopt) {
        auto value = NULL_VALUE;
        resolve_data_type(*expression_type, [&](auto type) { value = _evaluate_expression(type, expression); });

        if (!variant_is_null(value) &&
            _replace_expression_in_parents(node, node->output_column_references()[column_id], value)) {
          _remove_column_from_projection(projection_node, column_id);
        }
      }
    }

    column_id++;
  }

  return _apply_to_children(node);
}

bool NestedExpressionRule::_replace_expression_in_parents(const std::shared_ptr<AbstractLQPNode>& node,
                                                          const LQPColumnReference& column_reference,
                                                          const AllTypeVariant& value) {
  auto parent_tree_changed = false;
  for (auto parent : node->parents()) {
    if (parent->type() == LQPNodeType::Predicate) {
      auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(parent);

      if (is_lqp_column_reference(predicate_node->value()) &&
          boost::get<LQPColumnReference>(predicate_node->value()) == column_reference) {
        auto new_predicate_node = std::make_shared<PredicateNode>(predicate_node->column_reference(),
                                                                  predicate_node->predicate_condition(), value);
        predicate_node->replace_with(new_predicate_node);
        parent_tree_changed = true;
      }
    }

    parent_tree_changed |= _replace_expression_in_parents(parent, column_reference, value);
  }
  return parent_tree_changed;
}

void NestedExpressionRule::_remove_column_from_projection(const std::shared_ptr<ProjectionNode>& node,
                                                          ColumnID column_id) {
  auto column_expressions = node->column_expressions();
  column_expressions.erase(column_expressions.begin() + column_id);

  auto projection_node = std::make_shared<ProjectionNode>(column_expressions);
  node->replace_with(projection_node);
}

std::optional<DataType> NestedExpressionRule::_get_type_of_expression(
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
AllTypeVariant NestedExpressionRule::_evaluate_expression(boost::hana::basic_type<T> type,
                                                          const std::shared_ptr<LQPExpression>& expression) const {
  if (expression->type() == ExpressionType::Literal) {
    return AllTypeVariant(boost::get<T>(expression->value()));
  }

  if (!expression->is_arithmetic_operator()) {
    return NULL_VALUE;
  }

  const auto& arithmetic_operator_function = arithmetic_operator_function_from_expression<T>(expression->type());

  auto value = AllTypeVariant{};

  const auto& left_child = expression->left_child();
  const auto& right_child = expression->right_child();
  const auto left_is_literal = left_child->type() == ExpressionType::Literal;
  const auto right_is_literal = right_child->type() == ExpressionType::Literal;

  if ((left_is_literal && variant_is_null(left_child->value())) || (right_is_literal && variant_is_null(right_child->value()))) {
    // one of the operands is a literal null - early out.
    value = NULL_VALUE;

  } else if (left_is_literal && right_is_literal) {
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_child->value()), boost::get<T>(right_child->value())));

  } else if (right_is_literal) {
    auto left_value = _evaluate_expression(type, left_child);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_value), boost::get<T>(right_child->value())));

  } else if (left_is_literal) {
    auto right_value = _evaluate_expression(type, right_child);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_child->value()), boost::get<T>(right_value)));

  } else {
    auto left_value = _evaluate_expression(type, left_child);
    auto right_value = _evaluate_expression(type, right_child);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_value), boost::get<T>(right_value)));
  }

  return value;
}

}  // namespace opossum
