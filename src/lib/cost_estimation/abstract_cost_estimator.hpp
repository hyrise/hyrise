#pragma once

#include <memory>

#include "statistics/cardinality_estimation_cache.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class TableStatistics;
class AbstractCardinalityEstimator;

/**
 * Base class of an algorithm that predicts Cost for operators and plans.
 */
class AbstractCostEstimator {
 public:
  explicit AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& init_cardinality_estimator);
  virtual ~AbstractCostEstimator() = default;

  /**
   * Estimate the Cost of a (sub-)plan.
   * If `cost_estimation_by_lqp_cache` is enabled by calling `guarantee_bottom_up_construction()`:
   *     Tries to obtain subplan costs from `cost_estimation_by_lqp_cache`. Stores the cost for @param lqp in the
   *     `cost_estimation_by_lqp_cache` cache
   * @return The estimated cost of an @param lqp. Calls estimate_node_cost() on each individual node of the plan.
   */
  Cost estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  /**
   * @return the estimated cost of a single node. The `cost_estimation_by_lqp_cache` will not be used
   */
  virtual Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const = 0;

  /**
   * @return a new instance of this estimator with a new instance of the wrapped cardinality estimator, both with empty
   *         caches. Used so that caching guarantees can be enabled on the returned estimator.
   */
  virtual std::shared_ptr<AbstractCostEstimator> new_instance() const = 0;

  /**
   * Promises to the CostEstimator (and underlying CardinalityEstimator) that it will only be used to estimate bottom-up
   * constructed plans. That is, the Cost/Cardinality of a node, once constructed, never changes.
   * This enables the usage of a <lqp-ptr> -> <cost> cache (`cost_estimation_by_lqp_cache`).
   */
  void guarantee_bottom_up_construction();

  const std::shared_ptr<AbstractCardinalityEstimator> cardinality_estimator;

  mutable std::optional<std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>> cost_estimation_by_lqp_cache;

 private:
  /**
   * The Cost of a subplan can be retrieved from the `cost_estimation_by_lqp_cache` under two conditions:
   *    - It is, obviously, actually in the cache
   *    - No node in the subplan has already been costed and is marked as @param visited. This avoids incorporating
   *      the cost of nodes multiple times in the presence of diamond shapes in the plan.
   *
   * If the Cost for a subplan can be retrieved, all its nodes are marked as @param visisted.
   */
  std::optional<Cost> _get_subplan_cost_from_cache(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                   std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited) const;
};

}  // namespace opossum
