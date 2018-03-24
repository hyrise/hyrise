#include "abstract_expression.hpp"

#include <queue>

namespace opossum {

AbstractExpression::AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  type(type), arguments(arguments) {

}

bool AbstractExpression::requires_calculation() const {
  return !arguments.empty();
}

bool AbstractExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;
  if (!deep_equals_expressions(arguments, expression.arguments)) return false;
  return _shallow_equals(expression);
}

bool AbstractExpression::_shallow_equals(const AbstractExpression& expression) const {
  return true;
}

bool deep_equals_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a, const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b) {
  if (expressions_a.size() != expressions_b.size()) return false;
  for (auto expression_idx = size_t{0}; expression_idx < expressions_a.size(); ++expression_idx) {
    if (!expressions_a[expression_idx]->deep_equals(*expressions_b[expressions_b])) return false;
  }
  return true;
}

std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> copies;
  copies.reserve(expressions.size());
  for (const auto& expression : expressions) {
    copies.emplace_back(expression->deep_copy());
  }
  return copies;
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

}  // namespace opoosum