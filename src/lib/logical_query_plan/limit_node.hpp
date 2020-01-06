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

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  std::shared_ptr<AbstractExpression> num_rows_expression() const;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
