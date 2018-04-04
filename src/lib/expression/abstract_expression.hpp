#pragma once

#include <functional>
#include <memory>

#include "types.hpp"

namespace opossum {

enum class ExpressionType {
  Aggregate, Arithmetic, Array, Case, Column, Exists, Function, In, Logical, Not, Predicate, Select, Value, ValuePlaceholder
};

class AbstractExpression : public std::enable_shared_from_this<AbstractExpression> {
 public:
  explicit AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  virtual ~AbstractExpression() = default;

  bool deep_equals(const AbstractExpression& expression) const;

  virtual bool requires_calculation() const;
  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;

  virtual std::string description() const = 0;

  const ExpressionType type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

 protected:
  virtual bool _shallow_equals(const AbstractExpression& expression) const;
};

bool deep_equals_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions_a, const std::vector<std::shared_ptr<AbstractExpression>>& expressions_b);

std::vector<std::shared_ptr<AbstractExpression>> deep_copy_expressions(const std::vector<std::shared_ptr<AbstractExpression>>& expressions);

using ExpressionVisitor = std::function<bool(std::shared_ptr<AbstractExpression>&)>;

void visit_expression(std::shared_ptr<AbstractExpression>& expression, const ExpressionVisitor& visitor);

}  // namespace opossum
