#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class ArithmeticOperator {
  Addition,
  Subtraction,
  Multiplication,
  Division,
  Modulo,
  Power
};

class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(const ArithmeticOperator arithmetic_operator, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;

  ArithmeticOperator arithmetic_operator;

  std::string as_column_name() const override;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
