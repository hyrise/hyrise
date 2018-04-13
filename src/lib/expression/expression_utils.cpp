#include "expression_utils.hpp"

#include <sstream>
#include <queue>

#include "abstract_expression.hpp"
#include "lqp_column_expression.hpp"

namespace opossum {

bool expressions_equal(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                       const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  return false;
}

bool expressions_equal_to_expressions_in_different_lqp(
const std::vector<std::shared_ptr<AbstractExpression>> &expressions_left,
const std::vector<std::shared_ptr<AbstractExpression>> &expressions_right,
const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>> &node_mapping) {
  return false;
}

bool expressions_equal_to_expressions_in_different_lqp(const AbstractExpression& expression_left,
                       const AbstractExpression& expression_right,
                       const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping) {
  return false;
}


std::vector<std::shared_ptr<AbstractExpression>> expressions_copy(
const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  return {};
}

std::vector<std::shared_ptr<AbstractExpression>> expressions_copy_and_adapt_to_different_lqp(
const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping) {
  return {};
}

std::shared_ptr<AbstractExpression> expression_copy_and_adapt_to_different_lqp(
const AbstractExpression& expression,
const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping){
  return {};
}

void expression_adapt_to_different_lqp(
AbstractExpression& expression,
const std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>& node_mapping){

}

std::string expression_column_names(const std::vector<std::shared_ptr<AbstractExpression>> &expressions) {
  std::stringstream stream;
  for (auto expression_idx = size_t{0}; expression_idx < expressions.size(); ++expression_idx) {
    stream << expressions[expression_idx];
    if (expression_idx + 1 < expressions.size()) {
      stream << ", ";
    }
  }
  return stream.str();
}


}  // namespace opossum
