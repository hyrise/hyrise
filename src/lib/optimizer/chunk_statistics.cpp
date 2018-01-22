#include "chunk_statistics.hpp"

#include "utils/assert.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const ColumnID column_id, const AllTypeVariant& value, const ScanType scan_type) const {
  DebugAssert(column_id < _statistics.size(), "the passed column id should fit in the bounds of the statistics");
  return _statistics[column_id]->can_prune(value, scan_type);
}

} // namespace opossum
