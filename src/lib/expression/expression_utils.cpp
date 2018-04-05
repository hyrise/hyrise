#include "expression_utils.hpp"

#include <sstream>
#include <queue>

#include "abstract_expression.hpp"
#include "lqp_column_expression.hpp"

namespace opossum {

void AbstractLQPNode::adapt_expression_to_different_lqp(
AbstractExpression& expression, const AbstractLQPNode& original_lqp,
AbstractLQPNode& copied_lqp) {
  visit_expression(expression, [](auto& sub_expression) {
    if (sub_expression->type == ExpressionType::Column) {
      auto& column_reference = std::static_pointer_cast<LQPColumnExpression>(sub_expression)->column;
      column_reference = adapt_column_reference_to_different_lqp(column_reference, original_lqp, copied_lqp)
    }
    return true;
  });
}

bool deep_equals_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a, const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  if (expressions_a.size() != expressions_b.size()) return false;
  for (auto expression_idx = size_t{0}; expression_idx < expressions_a.size(); ++expression_idx) {
    if (!expressions_a[expression_idx]->deep_equals(*expressions_b[expressions_b])) return false;
  }
  return true;
}

bool deep_equals_expressions(const AbstractLQPNode& lqp_left,
                             const std::vector<std::shared_ptr<AbstractExpression>>& expressions_left,
                             const AbstractLQPNode& lqp_right,
                             const std::vector<std::shared_ptr<AbstractExpression>>& expressions_right);

bool deep_equals_expressions(const AbstractLQPNode& lqp_left, const AbstractExpression& expression_left,
                             const AbstractLQPNode& lqp_right, const AbstractExpression& expression_right);

std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> copies;
  copies.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copies.emplace_back(expression->deep_copy());
  }
  return copies;
}


std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(
const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
const AbstractLQPNode& original_lqp,
AbstractLQPNode& copied_lqp) {

  std::vector<std::shared_ptr<AbstractExpression>> copied_expressions;
  copied_expressions.reserve(expressions.size());
  for (const auto& expression : expressions) {
    auto expression_copy = expression->deep_copy();
    adapt_expression_to_different_lqp(*expression_copy, original_lqp, copied_lqp);
    copied_expressions.emplace_back(expression_copy);
  }

  return copied_expressions;
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

void visit_expression(std::shared_ptr<AbstractExpression>& expression, const ExpressionVisitor& visitor) {
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
