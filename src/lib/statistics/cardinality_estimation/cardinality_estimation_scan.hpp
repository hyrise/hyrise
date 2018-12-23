#pragma once

#include <memory>

namespace opossum {

template <typename T>
class AbstractHistogram;
class ChunkStatistics2;
template <typename T>
class GenericHistogram;
struct OperatorScanPredicate;
class TableStatistics2;

template <typename T>
std::shared_ptr<GenericHistogram<T>> estimate_histogram_of_column_to_column_equi_scan_with_bin_adjusted_histograms(
    const std::shared_ptr<AbstractHistogram<T>>& left_histogram,
    const std::shared_ptr<AbstractHistogram<T>>& right_histogram);

std::shared_ptr<ChunkStatistics2> cardinality_estimation_chunk_scan(
    const std::shared_ptr<ChunkStatistics2>& input_chunk_statistics, const OperatorScanPredicate& predicate);

}  // namespace opossum
