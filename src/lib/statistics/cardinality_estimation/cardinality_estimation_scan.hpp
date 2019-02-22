#pragma once

#include <memory>

namespace opossum {

template <typename T>
class AbstractHistogram;
class HorizontalStatisticsSlice;
template <typename T>
class GenericHistogram;
struct OperatorScanPredicate;

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_column_to_column_equi_scan(const AbstractHistogram<T>& left_histogram,
                                                                         const AbstractHistogram<T>& right_histogram);

std::shared_ptr<HorizontalStatisticsSlice> cardinality_estimation_scan_slice(
    const std::shared_ptr<HorizontalStatisticsSlice>& input_statistics_slice, const OperatorScanPredicate& predicate);

}  // namespace opossum
