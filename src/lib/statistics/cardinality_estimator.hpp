#pragma once

#include <memory>

#include "boost/dynamic_bitset.hpp"

#include "abstract_cardinality_estimator.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"

namespace opossum {

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

  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const;

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
}  // namespace opossum
