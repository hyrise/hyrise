#pragma once

#include <iostream>

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

std::ostream& operator<<(std::ostream& stream, const ArithmeticOperator arithmetic_operator);

class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(const ArithmeticOperator arithmetic_operator, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;
  bool is_nullable() const override;

  ArithmeticOperator arithmetic_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
