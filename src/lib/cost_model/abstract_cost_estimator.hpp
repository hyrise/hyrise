#pragma once

#include <memory>

#include "cost.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractCardinalityEstimator;
class OptimizationContext;

/**
 * Interface of an algorithm that predicts Cost for operators.
 */
class AbstractCostEstimator {
 public:
  explicit AbstractCostEstimator(const std::shared_ptr<AbstractCardinalityEstimator>& cardinality_estimator);
  virtual ~AbstractCostEstimator() = default;

  Cost estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp,
                          const std::shared_ptr<OptimizationContext>& context = {}) const;

  virtual Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node,
                                  const std::shared_ptr<OptimizationContext>& context) const = 0;

  std::shared_ptr<AbstractCardinalityEstimator> cardinality_estimator;
};

}  // namespace opossum
