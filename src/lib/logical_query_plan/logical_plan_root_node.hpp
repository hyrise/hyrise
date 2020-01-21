#pragma once

#include <string>

#include "abstract_lqp_node.hpp"

namespace opossum {

/**
 * This node is used in the Optimizer to have an explicit root node it can hold onto the LQP with.
 *
 * Optimizer rules are not allowed to remove this node or add nodes above it.
 *
 * By that Optimizer Rules don't have to worry whether they change the tree-identifying root node,
 * e.g. by removing the Projection at the top of the tree.
 */
class LogicalPlanRootNode : public EnableMakeForLQPNode<LogicalPlanRootNode>, public AbstractLQPNode {
 public:
  LogicalPlanRootNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& node_mapping) const override;
  bool _on_shallow_equals(const AbstractLQPNode& rhs, const LQPNodeMapping& node_mapping) const override;
};

}  // namespace opossum
