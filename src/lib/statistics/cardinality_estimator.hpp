#pragma once

#include <memory>

#include "boost/dynamic_bitset.hpp"

#include "abstract_cardinality_estimator.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/histograms/abstract_histogram.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
class HorizontalStatisticsSlice;
template <typename T>
class GenericHistogram;
template <typename T>
class VerticalStatisticsSlice;
class PredicateNode;

/**
 * Hyrise's default, statistics-based cardinality estimator
 */
class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  std::shared_ptr<AbstractCardinalityEstimator> clone_with_cache(
      const std::shared_ptr<CardinalityEstimationCache>& cardinality_estimation_cache) const override;

  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableCardinalityEstimationStatistics> estimate_statistics(
      const std::shared_ptr<AbstractLQPNode>& lqp) const;
};
}  // namespace opossum

