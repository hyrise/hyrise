#pragma once

#include "abstract_select_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class ExternalExpression;

class LQPSelectExpression : public AbstractSelectExpression {
 public:
  explicit LQPSelectExpression(const std::shared_ptr<AbstractLQPNode>& lqp,
                               const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions);

  std::shared_ptr<AbstractExpression> deep_copy() const override;
  std::string as_column_name() const override;

  const std::vector<std::shared_ptr<AbstractExpression>>& referenced_external_expressions() const;

  std::shared_ptr<AbstractLQPNode> lqp;

 protected:
  bool _shallow_equals(const AbstractExpression& expression) const override;
  size_t _on_hash() const override;
};

}  // namespace opossum
