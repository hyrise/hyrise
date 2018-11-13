#pragma once

#include <memory>

#include "boost/dynamic_bitset.hpp"

#include "abstract_cardinality_estimator.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/chunk_statistics/histograms/abstract_histogram.hpp"

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

  template <typename T>
  static std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_inner_equi_join_with_bin_adjusted_histograms(
      const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
      const std::shared_ptr<AbstractHistogram<T>>& histogram_right);

  template <typename T>
  static std::shared_ptr<GenericHistogram<T>>
  estimate_histogram_of_column_to_column_equi_scan_with_bin_adjusted_histograms(
      const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
      const std::shared_ptr<AbstractHistogram<T>>& histogram_right);

  static std::shared_ptr<ChunkStatistics2> estimate_scan_predicates_on_chunk(
      const std::shared_ptr<PredicateNode>& predicate_node,
      const std::shared_ptr<ChunkStatistics2>& input_chunk_statistics,
      const std::vector<OperatorScanPredicate>& operator_scan_predicates);

  static std::shared_ptr<TableStatistics2> estimate_cross_join(
      const std::shared_ptr<TableStatistics2>& left_input_table_statistics,
      const std::shared_ptr<TableStatistics2>& right_input_table_statistics);

  template <typename T>
  static std::shared_ptr<AbstractHistogram<T>> get_best_available_histogram(
      const SegmentStatistics2<T>& segment_statistics);
};

}  // namespace opossum

#include "cardinality_estimator.ipp"
