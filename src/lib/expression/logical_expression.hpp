#pragma once

#include <iostream>

#include "abstract_expression.hpp"

namespace opossum {

enum class LogicalOperator { And, Or };

std::ostream& operator<<(std::ostream& stream, const LogicalOperator logical_operator);

class LogicalExpression : public AbstractExpression {
 public:
  LogicalExpression(const LogicalOperator logical_operator, const std::shared_ptr<AbstractExpression>& left_operand,
                    const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;
  DataType data_type() const override;

  const LogicalOperator logical_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  ExpressionPrecedence _precedence() const override;
};

}  // namespace opossum
