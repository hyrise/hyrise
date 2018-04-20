#include "expression_utils.hpp"

#include <algorithm>
#include <sstream>
#include <queue>

#include "lqp_column_expression.hpp"

namespace opossum {

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return std::equal(expressions_a.begin(), expressions_a.end(), expressions_b.begin(), expressions_b.end(),
                    [&] (const auto& expression_a, const auto& expression_b) { return expression_a->deep_equals(*expression_b);});
}

//bool expressions_equal_to_expressions_in_different_lqp(
//const std::vector<std::shared_ptr<AbstractExpression>> &expressions_left,
//const std::vector<std::shared_ptr<AbstractExpression>> &expressions_right,
//const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>> &node_mapping) {
//  return false;
//}
//
//bool expressions_equal_to_expressions_in_different_lqp(const AbstractExpression& expression_left,
//                       const AbstractExpression& expression_right,
//                       const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping) {
//  return false;
//}


std::vector<std::shared_ptr<AbstractExpression>> expressions_copy(
const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression->deep_copy());
  }
  return copied_expressions;
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
const LQPNodeMapping& node_mapping) {
  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());

  for (const auto& expression : expressions) {
    copied_expressions.emplace_back(expression_copy_and_adapt_to_different_lqp(*expression, node_mapping));
  }

  return copied_expressions;
}

std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(
const AbstractExpression& expression,
const LQPNodeMapping& node_mapping) {
  auto copied_expression = expression.deep_copy();
  expression_adapt_to_different_lqp(copied_expression, node_mapping);
  return copied_expression;
}

//std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
//const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
//const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping) {
//  return {};
//}
//
//std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(
//const AbstractExpression& expression,
//const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping){
//  return {};
//}

void expression_adapt_to_different_lqp(
  std::shared_ptr<AbstractExpression>& expression,
  const LQPNodeMapping& node_mapping){

  visit_expression(expression, [&](auto& expression_ptr) {
    if (expression_ptr->type != ExpressionType::Column) return true;

    const auto lqp_column_expression_ptr = std::dynamic_pointer_cast<LQPColumnExpression>(expression_ptr);
    Assert(lqp_column_expression_ptr, "Asked to adapt expression in LQP, but encountered non-LQP ColumnExpression");

    expression_ptr = expression_adapt_to_different_lqp(*lqp_column_expression_ptr, node_mapping);

    return false;
  });
}

std::shared_ptr<LQPColumnExpression> expression_adapt_to_different_lqp(
const LQPColumnExpression& lqp_column_expression,
const LQPNodeMapping& node_mapping) {
  const auto node_mapping_iter = node_mapping.find(lqp_column_expression.column_reference.original_node());
  Assert(node_mapping_iter != node_mapping.end(), "Couldn't find referenced node in NodeMapping");

  LQPColumnReference adapted_column_reference{node_mapping_iter->second, lqp_column_expression.column_reference.original_column_id()};

  return std::make_shared<LQPColumnExpression>(adapted_column_reference);
}

std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression>> &expressions) {
  std::stringstream stream;
  for (auto expression_idx = size_t{0}; expression_idx < expressions.size(); ++expression_idx) {
    stream << expressions[expression_idx]->as_column_name();
    if (expression_idx + 1 < expressions.size()) {
      stream << ", ";
    }
  }
  return stream.str();
}


}  // namespace opossum
