#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

enum class ExpressionType {
  Aggregate, Arithmetic, Case, Column, Exists, Function, In, Logical, Placeholder, Select, Value
};

class AbstractExpression : public std::enable_shared_from_this<AbstractExpression>, private Noncopyable {
 public:
  explicit AbstractExpression(const ExpressionType type);

  virtual std::shared_ptr<AbstractExpression> deep_copy() const = 0;
  virtual std::shared_ptr<AbstractExpression> resolve_expression_columns() const = 0;

};

}  // namespace opossum
