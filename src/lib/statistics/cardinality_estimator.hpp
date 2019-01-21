#pragma once

#include <memory>

#include "boost/dynamic_bitset.hpp"

#include "abstract_cardinality_estimator.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/histograms/abstract_histogram.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class ChunkStatistics2;
template <typename T>
class GenericHistogram;
template <typename T>
class SegmentStatistics2;
class PredicateNode;

class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  static std::optional<boost::dynamic_bitset<>> build_plan_bitmask(const std::shared_ptr<AbstractLQPNode>& lqp,
                                                                   const std::shared_ptr<OptimizationContext>& context);

  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp,
                                   const std::shared_ptr<OptimizationContext>& context = {}) const override;

  std::shared_ptr<TableStatistics2> estimate_statistics(
      const std::shared_ptr<AbstractLQPNode>& lqp,
      const std::shared_ptr<OptimizationContext>& context = {}) const override;
};
}  // namespace opossum

#include "cardinality_estimator.ipp"
