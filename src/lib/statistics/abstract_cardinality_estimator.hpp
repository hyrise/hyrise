#pragma once

#include <memory>

#include "cardinality_estimation_cache.hpp"
#include "types.hpp"

namespace hyrise {

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
  virtual Cardinality estimate_cardinality(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                           const bool cacheable = true) const = 0;

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate Cardinalities of plans that consist
   * of the Vertices and Predicates in @param JoinGraph. This enables using the JoinGraphStatisticsCache during
   * Cardinality estimation.
   */
  void guarantee_join_graph(const JoinGraph& join_graph) const;

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate bottom-up constructed plans. That is,
   * the Cost/Cardinality of a node, once constructed, never changes. This enables the usage of a <lqp-ptr> -> <cost>
   * cache.
   */
  void guarantee_bottom_up_construction() const;

  /**
   * For increased cardinality estimation performance:
   * Extract columns that are required during cardinality estimations, e.g., columns used in join or selection
   * predicates. During estimations, only statistics for these columns are propagated.
   */
  void populate_required_column_expressions(const std::shared_ptr<const AbstractLQPNode>& lqp) const;

  mutable CardinalityEstimationCache cardinality_estimation_cache;

 protected:
  static void _add_required_columns(const std::shared_ptr<const AbstractLQPNode>& node,
                                    ExpressionUnorderedSet& required_columns);
};

}  // namespace hyrise
