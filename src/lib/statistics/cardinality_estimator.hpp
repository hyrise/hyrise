#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <vector>

#include <boost/dynamic_bitset.hpp>

#include "cardinality_estimation_cache.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/statistics_objects/generic_histogram_builder.hpp"
#include "types.hpp"

namespace hyrise {

template <typename T>
class AbstractHistogram;
template <typename T>
class GenericHistogram;
template <typename T>
class AttributeStatistics;
class AggregateNode;
class AliasNode;
class JoinNode;
class LimitNode;
class PredicateNode;
class ProjectionNode;
class UnionNode;
class ValidateNode;
class WindowNode;

/**
 * Hyrise's statistics-based cardinality estimator. It mostly relies on histograms that are taken from the base tables
 * and adapted (sliced, scaled) according to join and scan predicates along the query plan. The estimation process
 * happens recursively.
 *
 * To increase estimation performance, we cache already estimated statistics for (sub-)plans that are guaranteed not to
 * change anymore. This is beneficial if we call the CardinalityEstimator multiple times for the same LQP (e.g., during
 * join ordering, predicate reordering, etc.).
 * Even if we cannot cache statistics across multiple estimation invocations, we maintain a cache for a single call.
 * This helps if we have diamonds in the query plan (e.g., after predicate splitup or with semi-join reductions).
 * When allowing the CardinalityEstimator to use caching, you have to get a fresh instance (`new_instance()`). Thus, the
 * filled caches do not interfere with later estimations by, e.g., following optimizer rules.
 */
class CardinalityEstimator {
 public:
  using StatisticsByLQP = CardinalityEstimationCache::StatisticsByLQP;

  /**
   * @return A new instance of this estimator with empty caches. Used so that caching guarantees can be enabled on the
   *         returned estimator.
   */
  static std::shared_ptr<CardinalityEstimator> new_instance();

  /**
   * @return The estimated output row count of @param lqp.
   */
  Cardinality estimate_cardinality(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                   const bool cacheable = true) const;

  std::shared_ptr<TableStatistics> estimate_statistics(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                                       const bool cacheable = true) const;

  std::shared_ptr<TableStatistics> estimate_statistics(const std::shared_ptr<const AbstractLQPNode>& lqp,
                                                       const bool cacheable, StatisticsByLQP& statistics_cache) const;

  /**
   * Statistics caching
   * @{
   *
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate cardinalities of plans that consist
   * of the vertices and predicates in @param JoinGraph. This enables using the JoinGraphStatisticsCache during
   * cardinality estimation.
   */
  void guarantee_join_graph(const JoinGraph& join_graph);

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate bottom-up constructed plans. Thus, the
   * cardinalities/statistics of nodes, once constructed, never change. This enables the usage of an
   * <lqp-ptr> -> <statistics> cache.
   *
   * Image the following simple example of predicate reordering. Let's say we have a table R with 100'000 tuples, a
   * PredicateNode A with a selectivity of 0.3, and a PredicateNode B with a selectivity of 0.5. There are also more
   * nodes on top of the subplan. The inital structure (ordered by appearence in the SQL query) looks like that:
   *
   *             ...
   *              |
   *              | 15'000 (30'000 * 0.5)
   *              |
   *        PredicateNode B
   *              |
   *              | 30'000 (100'000 * 0.3)
   *              |
   *        PredicateNode A
   *              |
   *              | 100'000
   *              |
   *       StoredTableNode R
   *
   * First, we go top-down. As we already estimated nodes further up in the LQP, the statistics/cardinalities of the
   * PredicateNodes have been cached and will stay unchanged whenever we try to re-estimate them. When placed directly
   * above the StoredTableNode R, PredicateNode B yields a smaller cardinality than PredicateNode A even though it has a
   * worse selectivity, and we would falsely move it below PredicateNode A.
   *
   * Second, we go bottom-up. No statistics have been cached yet, we correctly estimate the cardinality based on the
   * selectivity, and we preserve the predicate order before continuing to the nodes further up. Note that during
   * predicate reordering, we do not cache the statistics of the PredicateNodes when deciding on their placement, but
   * only when we estimate nodes above.
   */
  void guarantee_bottom_up_construction();
  /** @} */

  /**
   * Per-node-type estimation functions
   * @{
   */
  static std::shared_ptr<TableStatistics> estimate_alias_node(
      const AliasNode& alias_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_projection_node(
      const ProjectionNode& projection_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_window_node(
      const WindowNode& window_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_aggregate_node(
      const AggregateNode& aggregate_node, const std::shared_ptr<TableStatistics>& input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_validate_node(
      const ValidateNode& /*validate_node*/, const std::shared_ptr<TableStatistics>& input_table_statistics);

  std::shared_ptr<TableStatistics> estimate_predicate_node(
      const PredicateNode& predicate_node, const std::shared_ptr<TableStatistics>& input_table_statistics,
      const bool cacheable, StatisticsByLQP& statistics_cache) const;

  static std::shared_ptr<TableStatistics> estimate_join_node(
      const JoinNode& join_node, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
      const std::shared_ptr<TableStatistics>& right_input_table_statistics);

  static std::shared_ptr<TableStatistics> estimate_union_node(
      const UnionNode& /*union_node*/, const std::shared_ptr<TableStatistics>& left_input_table_statistics,
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
     * Column-to-column scan estimation is notoriously hard; selectivities from 0 to 1 are possible for the same
     * histogram pairs. Thus, we do the most conservative estimation and compute the upper bound of value- and distinct
     * counts for each bin pair.
     */

    auto left_idx = BinID{0};
    auto right_idx = BinID{0};
    auto left_bin_count = left_histogram.bin_count();
    auto right_bin_count = right_histogram.bin_count();

    auto builder = GenericHistogramBuilder<T>{};

    while (left_idx < left_bin_count && right_idx < right_bin_count) {
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

    auto builder = GenericHistogramBuilder<T>{};

    // Iterate over both unified histograms and find overlapping bins.
    while (left_idx < left_bin_count && right_idx < right_bin_count) {
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

      // Overlapping bins found, estimate the join for these bins' range.
      const auto [height, distinct_count] = estimate_inner_equi_join_of_bins(
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

  mutable CardinalityEstimationCache cardinality_estimation_cache;
};

}  // namespace hyrise
