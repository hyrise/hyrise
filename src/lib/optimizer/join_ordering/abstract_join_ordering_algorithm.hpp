#pragma once

#include <memory>
#include <vector>

#include "cost_model/cost_estimation_cache.hpp"

namespace opossum {

class AbstractExpression;
class AbstractCardinalityEstimator;
class AbstractCostEstimator;
class AbstractLQPNode;
class JoinGraph;
class CardinalityEstimationCache;

class AbstractJoinOrderingAlgorithm {
 public:
  virtual ~AbstractJoinOrderingAlgorithm() = default;

 protected:
  std::shared_ptr<AbstractLQPNode> _add_predicates_to_plan(
      const std::shared_ptr<AbstractLQPNode>& lqp, const std::vector<std::shared_ptr<AbstractExpression>>& predicates,
      const std::shared_ptr<AbstractCostEstimator>& cost_estimator, const std::shared_ptr<CostEstimationCache>& cost_estimation_cache,
      const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const;

  std::shared_ptr<AbstractLQPNode> _add_join_to_plan(
      const std::shared_ptr<AbstractLQPNode>& left_lqp, const std::shared_ptr<AbstractLQPNode>& right_lqp,
      std::vector<std::shared_ptr<AbstractExpression>> join_predicates, const std::shared_ptr<AbstractCostEstimator>& cost_estimator,const std::shared_ptr<CostEstimationCache>& cost_estimation_cache,
      const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const;
};

}  // namespace opossum
