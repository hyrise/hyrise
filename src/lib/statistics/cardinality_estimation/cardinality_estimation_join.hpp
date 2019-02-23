#pragma once

#include "statistics/histograms/abstract_histogram.hpp"
#include "types.hpp"

/**
 * The functions declared here are called by the CardinalityEstimator. They are implemented as free functions in their
 * own translation unit to facilitate testing of the actual cardinality estimation algorithms and for separation of
 * concerns between scan- and join- estimation algorithms.
 */

namespace opossum {

struct OperatorJoinPredicate;
class TableCardinalityEstimationStatistics;
template <typename T>
class AbstractHistogram;
template <typename T>
class GenericHistogram;

namespace cardinality_estimation {

/**
 * Given two HistogramBins with equal bounds and the specified height and distinct counts, estimate the number of
 * matches and distinct values for a equi-inner join of these two bins using a principle-of-inclusion estimation.
 * @return {estimated_height, estimated_distinct_count}
 */
std::pair<HistogramCountType, HistogramCountType> bins_inner_equi_join(const float left_height,
                                                                       const float left_distinct_count,
                                                                       const float right_height,
                                                                       const float right_distinct_count);

/**
 * Estimate the inner-equi join of two histograms
 */
template <typename T>
std::shared_ptr<GenericHistogram<T>> histograms_inner_equi_join(const AbstractHistogram<T>& histogram_left,
                                                                const AbstractHistogram<T>& histogram_right);

/**
 * Estimate the inner-equi join of two table statistics
 */
std::shared_ptr<TableCardinalityEstimationStatistics> inner_equi_join(
    const ColumnID left_column_id, const ColumnID right_column_id,
    const TableCardinalityEstimationStatistics& left_input_table_statistics,
    const TableCardinalityEstimationStatistics& right_input_table_statistics);

/**
 * Estimate the cross join of two table statistics
 */
std::shared_ptr<TableCardinalityEstimationStatistics> cross_join(
    const TableCardinalityEstimationStatistics& left_input_table_statistics,
    const TableCardinalityEstimationStatistics& right_input_table_statistics);

}  // namespace cardinality_estimation

}  // namespace opossum
