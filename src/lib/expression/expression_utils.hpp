#pragma once

#include <functional>
#include <memory>
#include <vector>
#include <unordered_map>
#include <queue>

#include "abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                             const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b);

/**
 * Utility to compare vectors of Expressions from different LQPs
 */
//bool expressions_equal_to_expressions_in_different_lqp(
//const std::vector<std::shared_ptr<AbstractExpression>> &expressions_left,
//const std::vector<std::shared_ptr<AbstractExpression>> &expressions_right,
//const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>> &node_mapping);

/**
 * Utility to compare two Expressions from different LQPs
 */
//bool expression_equal_to_expression_in_different_lqp(const AbstractExpression& expression_left,
//                       const AbstractExpression& expression_right,
//                       const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping);


std::vector<std::shared_ptr<AbstractExpression>> expressions_copy(
  const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

//std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
//  const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
//  const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping);

//std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(
//  const AbstractExpression& expression,
//  const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping);

/**
 * Makes all ColumnExpressions points to their equivalent in a copied LQP
 */
//static void expression_adapt_to_different_lqp(
//  AbstractExpression& expression,
//  const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping);

std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression>> &expressions);

template<typename Visitor>
void visit_expression(std::shared_ptr<AbstractExpression>& expression, Visitor visitor){
  std::queue<std::reference_wrapper<std::shared_ptr<AbstractExpression>>> expression_queue;
  expression_queue.push(expression);

  while (!expression_queue.empty()) {
    auto expression_reference = expression_queue.front();
    expression_queue.pop();

    if (visitor(expression_reference.get())) {
      for (auto& argument : expression_reference.get()->arguments) {
        expression_queue.push(argument);
      }
    }
  }
}

}  // namespace opossum
