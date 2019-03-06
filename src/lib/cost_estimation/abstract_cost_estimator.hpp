#pragma once

#include <memory>

#include "cost_estimation_cache.hpp"
#include "statistics/cardinality_estimation_cache.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractCardinalityEstimator;

/**
 * Base class of an algorithm that predicts Cost for operators.
 */
class AbstractCostEstimator {
 public:
  explicit AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);
  virtual ~AbstractCostEstimator() = default;

  /**
   * Estimate the Cost of a (sub-)plan.
   * If `cost_estimation_cache.cost_by_lqp` is enabled:
   *     Tries to obtain subplan costs from `cost_estimation_cache`. Stores the cost for @param lqp in the
   *     `cost_estimation_cache` cache
   * @return The estimated cost of an @param lqp. Calls estimate_node_cost() on each individual node of the plan.
   */
  Cost estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  /**
   * @return the estimated cost of a single node. The `cost_estimation_cache` will not be used
   */
  Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const;

  /**
   * @return a clone of this estimator with a clone of the wrapped cardinality estimator. Used to then enable caching
   *         policies on the returned estimator.
   */
  virtual std::shared_ptr<AbstractCostEstimator> clone() const = 0;

  std::shared_ptr<AbstractCardinalityEstimator> cardinality_estimator;

  CostEstimationCache cost_estimation_cache;

 protected:
  virtual Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node, const std::) const;
};

}  // namespace opossum
