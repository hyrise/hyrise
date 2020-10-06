#pragma once

#include <iostream>

#include "../operators/property.hpp"
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

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const ArithmeticOperator arithmetic_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  bool _on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const override;
  ExpressionPrecedence _precedence() const override;

 public:
  // TODO(CAJan93): Support all relevant members, including parent members. Done?
  inline constexpr static auto properties = std::make_tuple(
      // from AbstractPredicateExpression
      property(&ArithmeticExpression::arithmetic_operator, "arithmetic_operator"),
      // From AbstractExpression
      property(&ArithmeticExpression::arguments, "arguments"), property(&ArithmeticExpression::type, "type"));
};

}  // namespace opossum
