#pragma once

#include <memory>
#include <string>

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractCostEstimator;
class AbstractLQPNode;
class LQPSubqueryExpression;

class AbstractRule {
 public:
  virtual ~AbstractRule() = default;

  /**
   * This function applies the concrete Optimizer Rule to an LQP.
   * The optimizer will pass the immutable LogicalPlanRootNode (@param lqp_root) to this function.
   */
  virtual void apply(const std::shared_ptr<AbstractLQPNode>& lqp_root) const;

  std::shared_ptr<AbstractCostEstimator> cost_estimator;

 protected:
  /**
   * _apply_to() is intended to be called recursively by the concrete rule.
   */
  virtual void _apply_to(const std::shared_ptr<AbstractLQPNode>& lqp_root) const = 0;

  /**
   * Calls _apply_to() for each input of @param node.
   *
   * IMPORTANT: Takes a copy of the node ptr because applying this rule to inputs of this node might remove this node
   * from the tree, which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  void _apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const;  // NOLINT
};

// All SubqueryExpressions referencing the same LQP
using SubqueryExpressionsByLQP =
    std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::vector<std::shared_ptr<LQPSubqueryExpression>>>>;

/**
 * Identifies unique LQPs and the (multiple) SubqueryExpressions referencing each of these unique LQPs.
 */
void collect_subquery_expressions_by_lqp(SubqueryExpressionsByLQP& subquery_expressions_by_lqp,
                                         const std::shared_ptr<AbstractLQPNode>& node,
                                         std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes =
                                             *std::make_unique<std::unordered_set<std::shared_ptr<AbstractLQPNode>>>());

}  // namespace opossum
