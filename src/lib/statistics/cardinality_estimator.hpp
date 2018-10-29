#pragma once

#include <memory>

#include "abstract_cardinality_estimator.hpp"
#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;
template <typename T>
class GenericHistogram;
template <typename T>
class SegmentStatistics2;

class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

  template <typename T>
  static std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_inner_equi_join_with_arithmetic_histograms(
      const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
      const std::shared_ptr<AbstractHistogram<T>>& histogram_right);

  template <typename T>
  static std::shared_ptr<AbstractHistogram<T>> get_best_available_histogram(
      const SegmentStatistics2<T>& segment_statistics);
};

}  // namespace opossum

#include "cardinality_estimator.ipp"
