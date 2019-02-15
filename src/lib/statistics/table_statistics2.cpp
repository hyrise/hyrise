#include "table_statistics2.hpp"

#include <numeric>

#include "table_statistics_slice.hpp"
#include "segment_statistics2.hpp"
#include "utils/assert.hpp"

namespace opossum {

Cardinality TableStatistics2::row_count() const {
  DebugAssert(!table_statistics_slice_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");

  return std::accumulate(table_statistics_slice_sets.front().begin(), table_statistics_slice_sets.front().end(), Cardinality{0},
                         [](const auto& a, const auto& chunk_statistics2) { return a + chunk_statistics2->row_count; });
}

size_t TableStatistics2::column_count() const {
  DebugAssert(!table_statistics_slice_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");
  DebugAssert(!table_statistics_slice_sets.front().empty(),
              "TableStatistics2 needs at least one ChunkStatistics for column_count()");
  return table_statistics_slice_sets.front().front()->segment_statistics.size();
}

DataType TableStatistics2::column_data_type(const ColumnID column_id) {
  DebugAssert(!table_statistics_slice_sets.empty(), "TableStatistics2 needs at least one ChunkStatisticsSet for row_count()");
  DebugAssert(!table_statistics_slice_sets.front().empty(),
              "TableStatistics2 needs at least one ChunkStatistics for column_count()");
  DebugAssert(column_id < column_count(), "ColumnID out of range");
  return table_statistics_slice_sets.front().front()->segment_statistics[column_id]->data_type;
}

std::ostream& operator<<(std::ostream& stream, const TableStatistics2& table_statistics) {
  stream << "TableStatistics - ChunkStatisticsSets: " << table_statistics.table_statistics_slice_sets.size() << " {" << std::endl;

  for (auto chunk_statistics_set_idx = size_t{0}; chunk_statistics_set_idx < table_statistics.table_statistics_slice_sets.size(); ++chunk_statistics_set_idx) {
    stream << "ChunkStatisticsSet " << chunk_statistics_set_idx << " {" << std::endl;

    const auto& chunk_statistics_set = table_statistics.table_statistics_slice_sets[chunk_statistics_set_idx];

    stream << chunk_statistics_set;

    stream << "} // ChunkStatisticsSet " << chunk_statistics_set_idx << std::endl;
  }

  stream << "} // TableStatistics" << std::endl;

  return stream;
}

std::ostream& operator<<(std::ostream& stream, const TableStatisticsSliceSet& chunk_statistics_set) {
  for (auto chunk_statistics_idx = size_t{0}; chunk_statistics_idx < chunk_statistics_set.size(); ++chunk_statistics_idx) {
    stream << "ChunkStatistics " << chunk_statistics_idx << " {" << std::endl;

    const auto& chunk_statistics = chunk_statistics_set[chunk_statistics_idx];

    stream << *chunk_statistics << std::endl;

    stream << "}" << std::endl;
  }

  return stream;
}

}  // namespace opossum
