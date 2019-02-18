#pragma once

#include <memory>

#include "cardinality.hpp"

namespace opossum {

class AbstractLQPNode;
class CardinalityEstimationCache;
class TableStatistics2;

class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  virtual std::shared_ptr<AbstractCardinalityEstimator> clone_with_cache(
      const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const = 0;

  virtual Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;
  virtual std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;

  std::shared_ptr<CardinalityEstimationCache> cardinality_estimation_cache;
};

}  // namespace opossum
