#include "table_statistics2.hpp"

#include <numeric>

#include "segment_statistics2.hpp"
#include "table_statistics_slice.hpp"
#include "utils/assert.hpp"

namespace opossum {

TableStatistics2::TableStatistics2(const std::vector<DataType>& column_data_types)
    : column_data_types(column_data_types) {}

Cardinality TableStatistics2::row_count() const {
  return std::accumulate(cardinality_estimation_slices.begin(), cardinality_estimation_slices.end(), Cardinality{0},
                         [](const auto& a, const auto& statistics_slice) { return a + statistics_slice->row_count; });
}

size_t TableStatistics2::column_count() const { return column_data_types.size(); }

std::ostream& operator<<(std::ostream& stream, const TableStatistics2& table_statistics) {
  stream << "TableStatistics {" << std::endl;
  stream << "ApproxInvalidRowCount: " << table_statistics.approx_invalid_row_count.load() << "; " << std::endl;

  if (!table_statistics.chunk_pruning_statistics.empty()) {
    stream << "Chunk Pruning Statistics {" << std::endl;
    for (const auto& [chunk_id, statistics_slice] : table_statistics.chunk_pruning_statistics) {
      stream << "Chunk " << chunk_id << ": {" << std::endl;
      stream << *statistics_slice;
      stream << "}  // Chunk " << chunk_id << std::endl;
    }
    stream << "} // Chunk Pruning Statistics" << std::endl;
  }

  if (!table_statistics.cardinality_estimation_slices.empty()) {
    stream << "Cardinality Estimation Statistics {" << std::endl;
    for (auto slice_idx = size_t{0}; slice_idx < table_statistics.cardinality_estimation_slices.size(); ++slice_idx) {
      stream << "Cardinality Estimation Slice " << slice_idx << ": {" << std::endl;
      stream << (*table_statistics.cardinality_estimation_slices[slice_idx]);
      stream << "} // Cardinality Estimation Slice " << slice_idx << std::endl;
    }
    stream << "} // Cardinality Estimation Statistics" << std::endl;
  }

  return stream;
}

}  // namespace opossum
