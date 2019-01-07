#include "table_statistics2.hpp"

#include <numeric>

#include "chunk_statistics2.hpp"
#include "segment_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cardinality TableStatistics2::row_count() const {
  return std::accumulate(chunk_statistics_primary.begin(), chunk_statistics_primary.end(), Cardinality{0},
                         [](const auto& a, const auto& chunk_statistics2) { return a + chunk_statistics2->row_count; });
}

size_t TableStatistics2::column_count() const {
  DebugAssert(!chunk_statistics_primary.empty(), "Need ChunkStatistics to determine number of columns");
  return chunk_statistics_primary.front()->segment_statistics.size();
}

DataType TableStatistics2::column_data_type(const ColumnID column_id) {
  DebugAssert(!chunk_statistics_primary.empty(), "Need ChunkStatistics to determine number of columns");
  DebugAssert(column_id < column_count(), "ColumnID out of range");
  return chunk_statistics_primary.front()->segment_statistics[column_id]->data_type;
}

const std::vector<std::shared_ptr<ChunkStatistics2>>& TableStatistics2::chunk_statistics_default() const {
  return chunk_statistics_primary;
}

const std::vector<std::shared_ptr<ChunkStatistics2>>& TableStatistics2::chunk_statistics_compact() const {
  return chunk_statistics_secondary ? *chunk_statistics_secondary : chunk_statistics_primary;
}

}  // namespace opossum
