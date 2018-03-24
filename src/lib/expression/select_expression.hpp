#pragma once

#include "abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;

class SelectExpression : public AbstractExpression {
 public:
  explicit SelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp);

  bool requires_calculation() const override;
  std::shared_ptr<AbstractExpression> deep_copy() const override;

  std::shared_ptr<AbstractLQPNode> lqp;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
};

}  // namespace opossum
