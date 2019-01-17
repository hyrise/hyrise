#pragma once

#include "abstract_lqp_node.hpp"
#include "enable_make_for_lqp_node.hpp"

namespace opossum {

/**
 * This node type represents validating tables with the Validate operator.
 */
class ValidateNode : public EnableMakeForLQPNode<ValidateNode>, public AbstractLQPNode {
 public:
  ValidateNode();

  std::string description() const override;

  std::shared_ptr<TableStatistics> derive_statistics_from(
      const std::shared_ptr<AbstractLQPNode>& left_input,
      const std::shared_ptr<AbstractLQPNode>& right_input = nullptr) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
