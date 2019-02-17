#pragma once

#include <memory>

#include "cost.hpp"
#include "cost_estimation_cache.hpp"
#include "statistics/cardinality_estimation_cache.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractCardinalityEstimator;

/**
 * Interface of an algorithm that predicts Cost for operators.
 */
class AbstractCostEstimator {
 public:
  explicit AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);
  virtual ~AbstractCostEstimator() = default;

  Cost estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const;
  virtual Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const = 0;

  virtual std::shared_ptr<AbstractCostEstimator> clone_with_caches(const std::shared_ptr<CostEstimationCache>& cost_estimation_cache, const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const = 0;

  std::shared_ptr<AbstractCardinalityEstimator> cardinality_estimator;
  std::shared_ptr<CostEstimationCache> cost_estimation_cache;
};

}  // namespace opossum
