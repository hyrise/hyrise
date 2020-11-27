#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace opossum {

class AbstractCardinalityEstimator;
class AbstractCostEstimator;
class AbstractLQPNode;
class LogicalPlanRootNode;
class LQPSubqueryExpression;

class AbstractRule {
 public:
  virtual ~AbstractRule() = default;

  /**
   * This function applies the concrete Optimizer Rule to an LQP.
   * Default implementation:
   *  (1) Optimize root LQP.
   *  (2) Optimize subquery LQPs of optimized root LQP, one-by-one.
   *
   *      IMPORTANT NOTES ON OPTIMIZING SUBQUERY LQPS:
   *
   *      Multiple Expressions in different nodes might reference the same LQP. Most commonly this will be the case
   *      for a ProjectionNode computing a subquery and a subsequent PredicateNode filtering based on it.
   *      We do not WANT to optimize the LQP twice (optimization takes time after all) and we CANNOT optimize it
   *      twice, since, e.g., a non-deterministic rule, could produce two different LQPs while optimizing and then the
   *      SubqueryExpression in the PredicateNode couldn't be resolved to a column anymore. There are more subtle
   *      ways LQPs might break in this scenario, and frankly, this is one of the weak links in the expression system...
   *
   *      ...long story short:
   *      !!!
   *      EACH UNIQUE SUB-LQP IS ONLY OPTIMIZED ONCE, EVEN IF IT OCCURS IN DIFFERENT NODES/EXPRESSIONS.
   *      !!!
   *
   * Rules can define their own strategy of optimizing subquery LQPs by overriding this function. See, for example, the
   * StoredTableColumnAlignmentRule.
   */
  virtual void apply_to(const std::shared_ptr<LogicalPlanRootNode>& lqp_root) const;

  std::shared_ptr<AbstractCostEstimator> cost_estimator;

 protected:
  /**
   * This function applies the concrete rule to a given plan. It is intended to be called recursively by the rule,
   * but for non-subquery-plans only.
   */
  virtual void _apply_recursively_to(const std::shared_ptr<AbstractLQPNode>& lqp_root) const = 0;

  /**
   * Calls _apply_recursively_to() for each input of @param node.
   *
   * IMPORTANT: Takes a copy of the node ptr because applying this rule to inputs of this node might remove this node
   * from the tree, which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  void _apply_recursively_to_inputs(std::shared_ptr<AbstractLQPNode> node) const;  // NOLINT
};

}  // namespace opossum
