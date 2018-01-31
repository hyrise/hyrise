#include "nested_expression_rule.hpp"

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "resolve_type.hpp"
#include "utils/arithmetic_operator_expression.hpp"

namespace opossum {

std::string NestedExpressionRule::name() const { return "Nested Expression Rule"; }

bool NestedExpressionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type() != LQPNodeType::Predicate || node->parents().size() > 1) {
    return _apply_to_children(node);
  }

  auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  if (!is_column_id(predicate_node->value())) {
    return _apply_to_children(node);
  }

  if (predicate_node->left_child()->type() != LQPNodeType::Projection ||
      predicate_node->parents().front()->type() != LQPNodeType::Projection) {
    return _apply_to_children(node);
  }

  auto projection_node_front = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->left_child());
  auto projection_node_back = std::dynamic_pointer_cast<ProjectionNode>(predicate_node->parents().front());

  if (projection_node_front->column_expressions().size() != projection_node_back->column_expressions().size() + 1) {
    return _apply_to_children(node);
  }

  auto expression = projection_node_front->column_expressions()[boost::get<ColumnID>(predicate_node->value())];

  const auto expression_type = _get_type_of_expression(expression);

  if (expression_type == DataType::Null) {
    return false;
  }

  auto value = NULL_VALUE;
  resolve_data_type(expression_type, [&](auto type) { value = _evaluate_expression(type, expression); });

  if (variant_is_null(value)) {
    return _apply_to_children(node);
  }

  auto new_predicate_node =
      std::make_shared<PredicateNode>(predicate_node->column_reference(), predicate_node->predicate_condition(), value);
  predicate_node->replace_with(new_predicate_node);

  projection_node_front->remove_from_tree();
  projection_node_back->remove_from_tree();

  return true;
}

DataType NestedExpressionRule::_get_type_of_expression(const std::shared_ptr<LQPExpression>& expression) const {
  if (expression->type() == ExpressionType::Literal) {
    return data_type_from_all_type_variant(expression->value());
  }

  if (!expression->is_arithmetic_operator()) {
    return DataType::Null;
  }

  const auto type_left = _get_type_of_expression(expression->left_child());
  const auto type_right = _get_type_of_expression(expression->right_child());

  if (type_left == DataType::Null) return type_right;
  if (type_left != type_right) return DataType::Null;

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

  const auto& left = expression->left_child();
  const auto& right = expression->right_child();
  const auto left_is_literal = left->type() == ExpressionType::Literal;
  const auto right_is_literal = right->type() == ExpressionType::Literal;

  if ((left_is_literal && variant_is_null(left->value())) || (right_is_literal && variant_is_null(right->value()))) {
    // one of the operands is a literal null - early out.
    value = NULL_VALUE;

  } else if (left_is_literal && right_is_literal) {
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left->value()), boost::get<T>(right->value())));

  } else if (right_is_literal) {
    auto left_value = _evaluate_expression(type, left);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_value), boost::get<T>(right->value())));

  } else if (left_is_literal) {
    auto right_value = _evaluate_expression(type, right);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left->value()), boost::get<T>(right_value)));

  } else {
    auto left_value = _evaluate_expression(type, left);
    auto right_value = _evaluate_expression(type, right);
    value = AllTypeVariant(arithmetic_operator_function(boost::get<T>(left_value), boost::get<T>(right_value)));
  }

  return value;
}

}  // namespace opossum
