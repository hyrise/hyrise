#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents limiting a result to a certain number of rows (LIMIT operator).
 */
class LimitNode : public EnableMakeForLQPNode<LimitNode>, public AbstractLQPNode {
 public:
  explicit LimitNode(const std::shared_ptr<AbstractExpression>& num_rows_expression);

  std::string description() const override;

  const std::shared_ptr<AbstractExpression> num_rows_expression;

 protected:
  std::shared_ptr<AbstractLQPNode> _shallow_copy_impl(LQPNodeMapping& node_mapping) const override;
  bool _shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
