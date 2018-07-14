#pragma once

#include "abstract_expression.hpp"

namespace opossum {

/**
 * Unary minus
 */
class UnaryMinusExpression : public AbstractExpression {
 public:
  explicit UnaryMinusExpression(const std::shared_ptr<AbstractExpression>& argument);

  std::shared_ptr<AbstractExpression> argument() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
