#pragma once

#include <functional>
#include <memory>
#include <vector>

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;

bool deep_equals_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a,
                             const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b);

std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(
  const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(
  const std::vector<std::shared_ptr<AbstractExpression>>& expressions,
  const AbstractLQPNode& original_lqp,
  AbstractLQPNode& copied_lqp);

/**
 * Makes all ColumnExpressions points to their equivalent in a copied LQP
 */
static void adapt_expression_to_different_lqp(
AbstractExpression& expression, const AbstractLQPNode& original_lqp,
AbstractLQPNode& copied_lqp);

std::string expressions_descriptions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

using ExpressionVisitor = std::function<bool(std::shared_ptr<AbstractExpression>&)>;

void visit_expression(std::shared_ptr<AbstractExpression>& expression, const ExpressionVisitor& visitor);

}  // namespace opossum
