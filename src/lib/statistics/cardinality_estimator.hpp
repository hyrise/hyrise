#pragma once

#include <memory>

#include <boost/dynamic_bitset.hpp>

#include "abstract_cardinality_estimator.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram_builder.hpp"

namespace hyrise {

template <typename T>
class AbstractHistogram;
template <typename T>
class GenericHistogram;
template <typename T>
class AttributeStatistics;
class AliasNode;
class ProjectionNode;
class AggregateNode;
class ValidateNode;
class PredicateNode;
class JoinNode;
class UnionNode;
class LimitNode;

/**
 * Hyrise's default, statistics-based cardinality estimator
 */
class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  std::shared_ptr<AbstractCardinalityEstimator> new_instance() const override;

  Cardinality estimate_cardinality(const std::shared_ptr<const AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics> estimate_statistics(const std::shared_ptr<const AbstractLQPNode>& lqp) const;

  /**
   * Per-node-type estimation functions
   * @{
   */
  static std::shared_ptr<TableStatistics> estimate_alias_node(
      const AliasNode& alias_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_projection_node(
      const ProjectionNode& projection_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_aggregate_node(
      const AggregateNode& aggregate_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_validate_node(
      const ValidateNode& validate_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_predicate_node(
      const PredicateNode& predicate_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_join_node(
      const JoinNode& join_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
      const std::shared_ptr<TableStatistics>& right_input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_union_node(
      const UnionNode& union_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
      const std::shared_ptr<TableStatistics>& right_input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_limit_node(
      const LimitNode& limit_node, const std::shared_ptr<TableStatistics>& input_table_statistics);
  /** @} */

  /**
   * Filter estimations
   * @{
   */

  /**
   * Estimate a simple scanning predicate. This function analyses the given predicate and dispatches the actual
   * estimation algorithm.
   */
  static std::shared_ptr<TableStatistics> estimate_operator_scan_predicate(
      const std::shared_ptr<TableStatistics>& input_table_statistics, const OperatorScanPredicate& predicate);

  /**
   * Estimation of an equi scan between two histograms. Estimating equi scans without correlation information is
   * impossible, so this function is restricted to computing an upper bound of the resulting histogram.
   */
  template <typename T>
  static std::shared_ptr<GenericHistogram<T>> estimate_column_vs_column_equi_scan_with_histograms(
      const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram) {
    /**
     * Column-to-column scan estimation is notoriously hard, selectivities from 0 to 1 are possible for the same histogram
     * pairs.
     * Thus, we do the most conservative estimation and compute the upper bound of value- and distinct counts for each
     * bin pair.
     */

    auto left_idx = BinID{0};
    auto right_idx = BinID{0};
    auto left_bin_count = left_histogram.bin_count();
    auto right_bin_count = right_histogram.bin_count();

    GenericHistogramBuilder<T> builder;

    for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
      const auto& left_min = left_histogram.bin_minimum(left_idx);
      const auto& right_min = right_histogram.bin_minimum(right_idx);

      if (left_min < right_min) {
        ++left_idx;
        continue;
      }

      if (right_min < left_min) {
        ++right_idx;
        continue;
      }

      DebugAssert(left_histogram.bin_maximum(left_idx) == right_histogram.bin_maximum(right_idx),
                  "Histogram bin boundaries do not match");

      const auto height = std::min(left_histogram.bin_height(left_idx), right_histogram.bin_height(right_idx));
      const auto distinct_count =
          std::min(left_histogram.bin_distinct_count(left_idx), right_histogram.bin_distinct_count(right_idx));

      if (height > 0 && distinct_count > 0) {
        builder.add_bin(left_min, left_histogram.bin_maximum(left_idx), height, distinct_count);
      }

      ++left_idx;
      ++right_idx;
    }

    if (builder.empty()) {
      return nullptr;
    }

    return builder.build();
  }

  /** @} */

  /**
   * Join estimations
   * @{
   */
  static std::shared_ptr<TableStatistics> estimate_inner_equi_join(const ColumnID left_column_id,
                                                                   const ColumnID right_column_id,
                                                                   const TableStatistics& left_input_table_statistics,
                                                                   const TableStatistics& right_input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_semi_join(const ColumnID left_column_id,
                                                             const ColumnID right_column_id,
                                                             const TableStatistics& left_input_table_statistics,
                                                             const TableStatistics& right_input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_cross_join(const TableStatistics& left_input_table_statistics,
                                                              const TableStatistics& right_input_table_statistics);

  template <typename T>
  static std::shared_ptr<GenericHistogram<T>> estimate_inner_equi_join_with_histograms(
      const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram) {
    /**
     * left_histogram and right_histogram are turned into "unified" histograms by `split_at_bin_bounds`, meaning that
     * their bins are split so that their bin boundaries match.
     * E.g., if left_histogram has a single bin [1, 10] and right histogram has a single bin [5, 20] then
     * unified_left_histogram == {[1, 4], [5, 10]}
     * unified_right_histogram == {[5, 10], [11, 20]}
     * The estimation is performed on overlapping bins only, e.g., only the two bins [5, 10] will produce matches.
     */

    auto unified_left_histogram = left_histogram.split_at_bin_bounds(right_histogram.bin_bounds());
    auto unified_right_histogram = right_histogram.split_at_bin_bounds(left_histogram.bin_bounds());

    auto left_idx = BinID{0};
    auto right_idx = BinID{0};
    auto left_bin_count = unified_left_histogram->bin_count();
    auto right_bin_count = unified_right_histogram->bin_count();

    GenericHistogramBuilder<T> builder;

    // Iterate over both unified histograms and find overlapping bins
    for (; left_idx < left_bin_count && right_idx < right_bin_count;) {
      const auto& left_min = unified_left_histogram->bin_minimum(left_idx);
      const auto& right_min = unified_right_histogram->bin_minimum(right_idx);

      if (left_min < right_min) {
        ++left_idx;
        continue;
      }

      if (right_min < left_min) {
        ++right_idx;
        continue;
      }

      DebugAssert(unified_left_histogram->bin_maximum(left_idx) == unified_right_histogram->bin_maximum(right_idx),
                  "Histogram bin boundaries do not match");

      // Overlapping bins found, estimate the join for these bins' range
      const auto [height, distinct_count] = estimate_inner_equi_join_of_bins(  // NOLINT
          unified_left_histogram->bin_height(left_idx), unified_left_histogram->bin_distinct_count(left_idx),
          unified_right_histogram->bin_height(right_idx), unified_right_histogram->bin_distinct_count(right_idx));

      if (height > 0) {
        builder.add_bin(left_min, unified_left_histogram->bin_maximum(left_idx), height, distinct_count);
      }

      ++left_idx;
      ++right_idx;
    }

    return builder.build();
  }

  /**
   * Given two HistogramBins with equal bounds and the specified height and distinct counts, estimate the number of
   * matches and distinct values for an equi-inner join of these two bins using a principle-of-inclusion estimation.
   * @return {estimated_height, estimated_distinct_count}
   */
  static std::pair<HistogramCountType, HistogramCountType> estimate_inner_equi_join_of_bins(
      const float left_height, const float left_distinct_count, const float right_height,
      const float right_distinct_count);

  /** @} */

  /**
   * Helper
   * @{
   */
  static std::shared_ptr<TableStatistics> prune_column_statistics(
      const std::shared_ptr<TableStatistics>& table_statistics, const std::vector<ColumnID>& pruned_column_ids);

  /** @} */
};
}  // namespace hyrise
