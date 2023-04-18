#pragma once

#include "abstract_lqp_node.hpp"

namespace hyrise {

/**
 * This node is used in the Optimizer to have an explicit root node that holds an LQP. Optimizer rules are not allowed
 * to remove this node or add nodes above it. By that, optimizer rules don't have to worry whether they change the
 * tree-identifying root node, e.g., by removing the Projection at the top of the tree.
 *
 * Moreover, optimizer rules can utilize this node during their runtime for, e.g., recursion purposes.
 * (See PredicatePlacementRule, for example)
 */
class LogicalPlanRootNode : public EnableMakeForLQPNode<LogicalPlanRootNode>, public AbstractLQPNode {
 public:
  LogicalPlanRootNode();

  std::string description(const DescriptionMode mode = DescriptionMode::Short) const override;

  UniqueColumnCombinations unique_column_combinations() const override;

  OrderDependencies order_dependencies() const override;

  FunctionalDependencies non_trivial_functional_dependencies() const override;

 protected:
  std::shared_ptr<AbstractLQPNode> _on_shallow_copy(LQPNodeMapping& /*node_mapping*/) const override;
  bool _on_shallow_equals(const AbstractLQPNode& /*rhs*/, const LQPNodeMapping& /*node_mapping*/) const override;
};

}  // namespace hyrise
