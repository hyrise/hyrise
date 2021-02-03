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
   * The default implementation
   *  (1) optimizes the root LQP
   *  (2) optimizes all (nested) subquery LQPs of the optimized root LQP, one-by-one.
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
  virtual void apply_to_plan(const std::shared_ptr<LogicalPlanRootNode>& lqp_root) const;

  std::shared_ptr<AbstractCostEstimator> cost_estimator;

  /**
   * Each rule needs to specify if it could potentially prevent caching.
   * This info is used for caching of parameterized plans in order to make sure that
   * an instantiation with new parameters of an already optimized and cached plan is
   * still a valid optimization.
   * A rule does not prevent caching if for every possible input lqp, the input
   * and output lqp of the rule yield the same result even if the parameters are
   * changed afterwards.
   * Consequently, all rules that alter the number of parameters or make optimizations
   * based on the actual values of the parameters prevent caching.
   * Each new rule should be thoroughly examined if it could potentially prevent caching.
   */
  virtual bool prevents_caching() const = 0;

 protected:
  /**
   * This function applies the concrete rule to the given plan, but not to its subquery plans.
   * To traverse LQPs, use the visit_lqp function. Do not call this function recursively.
   */
  virtual void _apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const = 0;

  /**
   * LEGACY FUNCTION (see #2276): Use visit_lqp for LQP traversal instead.
   *
   * Calls _apply_to_plan_without_subqueries() for each input of @param node.
   * IMPORTANT: Takes a copy of the node ptr because applying this rule to inputs of this node might remove this node
   * from the tree, which might result in this node being deleted if we don't take a copy of the shared_ptr here.
   */
  void _apply_to_plan_inputs_without_subqueries(std::shared_ptr<AbstractLQPNode> node) const;  // NOLINT
};

}  // namespace opossum
