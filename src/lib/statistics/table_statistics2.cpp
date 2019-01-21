#include "table_statistics2.hpp"

#include <numeric>

#include "chunk_statistics2.hpp"
#include "segment_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cardinality TableStatistics2::row_count() const {
  DebugAssert(!chunk_statistics_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");

  return std::accumulate(chunk_statistics_sets.front().begin(), chunk_statistics_sets.front().end(), Cardinality{0},
                         [](const auto& a, const auto& chunk_statistics2) { return a + chunk_statistics2->row_count; });
}

size_t TableStatistics2::column_count() const {
  DebugAssert(!chunk_statistics_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");
  DebugAssert(!chunk_statistics_sets.front().empty(),
              "TableStatistics2 needs at least one ChunkStatistics for column_count()");
  return chunk_statistics_sets.front().front()->segment_statistics.size();
}

DataType TableStatistics2::column_data_type(const ColumnID column_id) {
  DebugAssert(!chunk_statistics_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");
  DebugAssert(!chunk_statistics_sets.front().empty(),
              "TableStatistics2 needs at least one ChunkStatistics for column_count()");
  DebugAssert(column_id < column_count(), "ColumnID out of range");
  return chunk_statistics_sets.front().front()->segment_statistics[column_id]->data_type;
}

}  // namespace opossum
