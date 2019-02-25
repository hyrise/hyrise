#pragma once

#include <memory>

/**
 * The functions declared here are called by the CardinalityEstimator. They are implemented as free functions in their
 * own translation unit to facilitate testing of the actual cardinality estimation algorithms and for separation of
 * concerns between scan- and join- estimation algorithms.
 */

namespace opossum {

template <typename T>
class AbstractHistogram;
class HorizontalStatisticsSlice;
template <typename T>
class GenericHistogram;
struct OperatorScanPredicate;

namespace cardinality_estimation {

/**
 * Estimation of an equi scan between two histograms. Estimating equi scans without correlation information is
 * impossible, so this function is restricted to computing an upper bound of the resulting histogram.
 */
template <typename T>
std::shared_ptr<GenericHistogram<T>> histograms_column_vs_column_equi_scan(const AbstractHistogram<T>& left_histogram,
                                                                           const AbstractHistogram<T>& right_histogram);

/**
 * Estimate a simple scanning predicate. This function analyses the given predicate and dispatches the actual estimation
 * algorithm
 */
std::shared_ptr<HorizontalStatisticsSlice> operator_scan_predicate(
    const std::shared_ptr<HorizontalStatisticsSlice>& input_horizontal_slice, const OperatorScanPredicate& predicate);

}  // namespace cardinality_estimation

}  // namespace opossum
