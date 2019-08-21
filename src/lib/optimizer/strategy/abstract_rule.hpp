#pragma once

#include <memory>
#include <string>

namespace opossum {

class AbstractLQPNode;
class AbstractCostEstimator;
class AbstractCardinalityEstimator;

class AbstractRule {
 public:
  virtual ~AbstractRule() = default;

  /**
   * This function applies the concrete Optimizer Rule to an LQP.
   * apply_to() is intended to be called recursively by the concrete rule.
   * The optimizer will pass the immutable LogicalPlanRootNode to this function.
   */
  virtual void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const = 0;

  std::shared_ptr<AbstractCostEstimator> cost_estimator;

 protected:
  /**
   * Apply this rule to @param node's inputs and all subqueries in its expressions
   *
   * IMPORTANT: Takes a copy of the node ptr because applying this rule to inputs of this node might remove this node
   * from the tree, which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  void _apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const;  // NOLINT
};

}  // namespace opossum
