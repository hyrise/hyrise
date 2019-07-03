#pragma once

#include <memory>

#include "cardinality_estimation_cache.hpp"
#include "types.hpp"

namespace opossum {

class AbstractLQPNode;
class TableStatistics;

/**
 * Base class for algorithms determining the output cardinality/statistics of an LQP during optimization.
 */
class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  /**
   * @return a new instance of this estimator with empty caches. Used so that caching guarantees can be enabled on the
   * returned estimator.
   */
  virtual std::shared_ptr<AbstractCardinalityEstimator> new_instance() const = 0;

  /**
   * @return the estimated output row count of @param lqp
   */
  virtual Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate Cardinalities of plans that consist
   * of the Vertices and Predicates in @param JoinGraph. This enables using the JoinGraphStatisticsCache during
   * Cardinality estimation.
   */
  void guarantee_join_graph(const JoinGraph& join_graph);

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate bottom-up
   * constructed plans. That is, the Cost/Cardinality of a node, once constructed, never changes.
   * This enables the usage of a <lqp-ptr> -> <cost> cache.
   */
  void guarantee_bottom_up_construction();

  mutable CardinalityEstimationCache cardinality_estimation_cache;
};

}  // namespace opossum
