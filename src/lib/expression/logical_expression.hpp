#pragma once

#include <iostream>

#include "abstract_expression.hpp"

namespace hyrise {

enum class LogicalOperator { And, Or };

std::ostream& operator<<(std::ostream& stream, const LogicalOperator logical_operator);

class LogicalExpression : public AbstractExpression {
 public:
  LogicalExpression(const LogicalOperator init_logical_operator,
                    const std::shared_ptr<AbstractExpression>& left_operand,
                    const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> _on_deep_copy(
      std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const override;
  std::string description(const DescriptionMode mode) const override;
  DataType data_type() const override;

  const LogicalOperator logical_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _shallow_hash() const override;
  ExpressionPrecedence _precedence() const override;
};

}  // namespace hyrise
