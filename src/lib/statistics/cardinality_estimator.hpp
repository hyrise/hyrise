#pragma once

#include <algorithm>
#include <memory>
#include <ostream>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/dynamic_bitset.hpp>

#include "statistics/base_attribute_statistics.hpp"
#include "statistics/join_graph_statistics_cache.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "types.hpp"

namespace hyrise {

template <typename T>
class GenericHistogram;
template <typename T>
class AttributeStatistics;
class AbstractLQPNode;
class AggregateNode;
class AliasNode;
class JoinNode;
class LimitNode;
struct OperatorScanPredicate;
class PredicateNode;
class ProjectionNode;
class TableStatistics;
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
 * Even if we cannot cache statistics across multiple estimation invocations because the plan structure changes in
 * between, we maintain a cache for the scope of a single call to facilitate diamonds.
 * This helps if we have diamonds in the query plan (e.g., after predicate splitup or with semi-join reductions).
 * When allowing the CardinalityEstimator to use caching, you have to get a fresh instance (`new_instance()`). Thus, the
 * filled caches do not interfere with later estimations by, e.g., following optimizer rules.
 */
class CardinalityEstimator {
 public:
  using StatisticsByLQP = std::unordered_map<std::shared_ptr<const AbstractLQPNode>, std::shared_ptr<TableStatistics>>;

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
   * We use dummy objects for pruned statistics and cases where we do not estimate statistics (e.g., for aggregations).
   * Doing so has two advantages. First, it is semantically clear and easy to test. Second, we can easily prevent
   * creations of empty statistics objects when scaling dummy statistics.
   */
  class DummyStatistics : public BaseAttributeStatistics, public std::enable_shared_from_this<DummyStatistics> {
   public:
    explicit DummyStatistics(const DataType init_data_type);

    void set_statistics_object(const std::shared_ptr<const AbstractStatisticsObject>& /*statistics_object*/) override;

    std::shared_ptr<const BaseAttributeStatistics> scaled(const Selectivity /*selectivity*/) const override;

    std::shared_ptr<const BaseAttributeStatistics> sliced(
        const PredicateCondition /*predicate_condition*/, const AllTypeVariant& /*variant_value*/,
        const std::optional<AllTypeVariant>& /*variant_value2*/ = std::nullopt) const override;
  };

  // Helper to ensure no statistics for required LQPColumnExpressions were pruned. Should be adapted if we estimate
  // aggregations, window functions, or computed projections.
  static void assert_required_statistics(const ColumnID column_id, const std::shared_ptr<AbstractLQPNode>& input_node,
                                         const std::shared_ptr<const TableStatistics>& input_statistics);

  /**
   * Statistics caching
   * @{
   *
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate cardinalities of plans that consist
   * of the vertices and predicates in @param JoinGraph. This enables using the JoinGraphStatisticsCache during
   * cardinality estimation.
   */
  void guarantee_join_graph(const JoinGraph& join_graph) const;

  /**
   * For increased cardinality estimation performance:
   * Promises to this CardinalityEstimator that it will only be used to estimate bottom-up constructed plans. Thus, the
   * cardinalities/statistics of nodes, once constructed, never change. This enables the usage of an
   * <lqp-ptr> -> <statistics> cache.
   * Furthermore, this call also enables statistics pruning and population of columns that must not be pruned. Thus, it
   * sets @param lqp in the cache to lazily use it for populating the columns. This passed LQP node should be the root
   * of the (sub-)plan you wish to perform estimations for. If you do not wish to use statistics pruning (e.g., because
   * you untie nodes from the plan while performing estimations), call `do_not_prune_unused_statistics()` afterwards.
   *
   * Image the following simple example of predicate reordering. Assume we have a table R with 100'000 tuples, a
   * PredicateNode A with a selectivity of 0.3, and a PredicateNode B with a selectivity of 0.5. There are also more
   * nodes on top of the subplan. The inital structure (ordered by appearence in the SQL query) looks like that:
   *
   *             ...
   *              |
   *              | 15'000 (30'000 * 0.5)
   *              |
   *      [ PredicateNode B ]
   *              |
   *              | 30'000 (100'000 * 0.3)
   *              |
   *      [ PredicateNode A ]
   *              |
   *              | 100'000
   *              |
   *     [ StoredTableNode R ]
   *
   * Assume we performed predicate reordering top-down with caching enabled and now arrived at the predicate chain
   * shown in the example. As we already estimated nodes further up in the LQP, the statistics/cardinalities of the
   * PredicateNodes have been cached due to recursive input estimations. Cached statistics will stay unchanged whenever
   * we try to re-estimate them.
   * For predicate reordering, we place each PredicateNode directly above the StoredTableNode R, estimate the
   * cardinality, and order the nodes such that the node with the lowest cost/cardinality is executed first. Due to
   * caching, PredicateNode B yields a smaller cardinality than PredicateNode A even though it has a worse selectivity,
   * and we would erroneously move it below PredicateNode A.
   *
   * Thus, we go bottom-up to allow caching in the actual rule. When estimating the PredicateNodes, no statistics have
   * been cached yet for them. We correctly estimate the cardinalities based on the selectivities and we preserve the
   * predicate order before continuing to the nodes further up. Note that during predicate reordering, we do not cache
   * the statistics of the PredicateNodes while deciding on their placement, but only when we estimate nodes above,
   * where already visited subplans will not change anymore.
   *
   * tl;dr: When you need multiple estimations for the same LQP, you can only safely go top-down AND use caching when
   *        the cardinalities of nodes below do not change. To keep optimization costs low, it is best practice to
   *        recursively go bottom-up if you change the query plan in a way that influences intermediate cardinalities.
   */
  void guarantee_bottom_up_construction(const std::shared_ptr<const AbstractLQPNode>& lqp) const;

  /**
   * Prune statistics that are not relevant for cardinality estimation. Thus, we avoid forwarding and scaling
   * histograms. When caching is allowed, `guarantee_bottom_up_construction()` takes care that all predicates of the
   * final LQP are present in the cache.
   *
   * We do not prune statistics for single estimation calls without statistics caching enabled because we cannot easily
   * collect required statistics while recursing into the query plan: There might be diamonds where different paths
   * require different expressions. One branch of the diamond reaches the common input branch first, and not all
   * required expressions have been collected yet. Because we still cache estimated statistics during the estimation of
   * a single LQP, these expressions will be missing when the other branch of the diamond requests the estimated
   * statistics of the common input.
   *
   *    [ StoredTableNode ] -+---> [ PredicateNode a = 1 ] ---+-> [ UnionPositions ]
   *                          \                              /
   *                           +-> [ PredicateNode b = 2 ] -+
   *
   *
   * For the same reason, turn off pruning if you untie nodes while you perform estimations (e.g., in
   * PredicatePlacementRule)!
   */
  void prune_unused_statistics() const;
  void do_not_prune_unused_statistics() const;
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
      const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram);

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
      const AbstractHistogram<T>& left_histogram, const AbstractHistogram<T>& right_histogram);

  /**
   * Given two HistogramBins with equal bounds and the specified height and distinct counts, estimate the number of
   * matches and distinct values for an equi-inner join of these two bins using a principle-of-inclusion estimation.
   * @return {estimated_height, estimated_distinct_count}
   */
  static std::pair<HistogramCountType, HistogramCountType> estimate_inner_equi_join_of_bins(
      const HistogramCountType left_height, const DistinctCount left_distinct_count,
      const HistogramCountType right_height, const DistinctCount right_distinct_count);
  /** @} */

 private:
  friend class Optimizer;
  friend class OptimizerTest_PollutedCardinalityEstimationCache_Test;
  friend class CardinalityEstimatorTest_StatisticsCaching_Test;
  friend class CardinalityEstimatorTest_StatisticsPruning_Test;
  friend class CostEstimatorLogicalTest_CardinalityCaching_Test;

  struct CardinalityEstimationCache {
    std::optional<JoinGraphStatisticsCache> join_graph_statistics_cache;

    std::optional<StatisticsByLQP> statistics_by_lqp;

    std::optional<ExpressionUnorderedSet> required_column_expressions;

    std::shared_ptr<const AbstractLQPNode> lqp;
  };

  void _populate_required_column_expressions() const;

  mutable CardinalityEstimationCache cardinality_estimation_cache;
};

std::ostream& operator<<(std::ostream& stream, const CardinalityEstimator::DummyStatistics& /*dummy_statistics*/);

}  // namespace hyrise
