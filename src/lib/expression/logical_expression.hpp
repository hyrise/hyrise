#pragma once

#include "abstract_expression.hpp"

namespace opossum {

enum class LogicalOperator {
  And,
  Or,
  Not
};

class LogicalExpression : public AbstractExpression {
 public:
  LogicalExpression(const LogicalOperator logical_operator,
                    const std::shared_ptr<AbstractExpression>& left_operand,
                    const std::shared_ptr<AbstractExpression>& right_operand);

  const std::shared_ptr<AbstractExpression>& left_operand() const;
  const std::shared_ptr<AbstractExpression>& right_operand() const;

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string description() const override;

  LogicalOperator logical_operator;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
