#pragma once

#include <functional>
#include <memory>

#include "types.hpp"

namespace opossum {

enum class ExpressionType {
  Aggregate, Arithmetic, Array, Case, Column, Exists, External, Function, In, Logical, Not, Predicate, Select, Value, ValuePlaceholder
};

class AbstractExpression : public std::enable_shared_from_this<AbstractExpression> {
 public:
  explicit AbstractExpression(const ExpressionType type, const std::vector<std::shared_ptr<AbstractExpression>>& arguments);
  virtual ~AbstractExpression() = default;

  bool deep_equals(const AbstractExpression& expression) const;

  virtual bool requires_calculation() const;

  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;

  virtual std::string as_column_name() const = 0;

  size_t hash() const;

  const ExpressionType type;
  std::vector<std::shared_ptr<AbstractExpression>> arguments;

 protected:
  virtual bool _shallow_equals(const AbstractExpression& expression) const;
  virtual size_t _on_hash() const;
};

}  // namespace opossum
