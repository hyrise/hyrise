#pragma once

#include <iostream>

#include "abstract_expression.hpp"

namespace opossum {

enum class ArithmeticOperator { Addition, Subtraction, Multiplication, Division, Modulo };

std::ostream& operator<<(std::ostream& stream, const ArithmeticOperator arithmetic_operator);

/**
 * E.g. `2+3`, `5*3*4`
 */
class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(const ArithmeticOperator init_arithmetic_operator,
                       const std::shared_ptr<AbstractExpression>& left_operand,
                       const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const ArithmeticOperator arithmetic_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
  ExpressionPrecedence _precedence() const override;
};

}  // namespace opossum
