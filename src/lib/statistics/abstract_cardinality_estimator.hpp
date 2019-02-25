#pragma once

#include <memory>

#include "cardinality.hpp"

namespace opossum {

class AbstractLQPNode;
class CardinalityEstimationCache;
class TableCardinalityEstimationStatistics;

/**
 * Interface for algorithms determining the output cardinality/statistics of an LQP during optimization.
 */
class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  virtual std::shared_ptr<AbstractCardinalityEstimator> clone_with_cache(
      const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const = 0;

  virtual Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;

  // Optional, can be used by the derived class
  std::shared_ptr<CardinalityEstimationCache> cardinality_estimation_cache;
};

}  // namespace opossum
