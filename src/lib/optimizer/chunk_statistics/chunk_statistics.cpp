#include "chunk_statistics.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value,
                                const PredicateCondition scan_type) const {
  DebugAssert(column_id < _chunk_column_statistics.size(), "The passed column ID should fit in the bounds of the statistics.");
  DebugAssert(_chunk_column_statistics[column_id], "The statistics should not contain any empty shared_ptrs.");
  return _chunk_column_statistics[column_id]->can_prune(value, scan_type);
}

}  // namespace opossum
