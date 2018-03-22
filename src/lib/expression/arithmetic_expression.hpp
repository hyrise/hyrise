#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class ArithmeticOperator {
  Addition,
  Subtraction,
  Multiplication,
  Division
};

class ArithmeticExpression : public AbstractExpression {
 public:
  ArithmeticExpression(const ArithmeticOperator arithmetic_operator, const std::shared_ptr<AbstractExpression>& left_operand, const std::shared_ptr<AbstractExpression>& right_operand);

  /**
   * @defgroup Overrides for AbstractExpression
   * @{
   */
  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::shared_ptr<AbstractExpression> resolve_expression_columns() const override;
  /**@}*/

  ArithmeticOperator arithmetic_operator;
  std::shared_ptr<AbstractExpression> left_operand;
  std::shared_ptr<AbstractExpression> right_operand;
};

}  // namespace opossum
