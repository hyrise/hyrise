#include "chunk_statistics.hpp"

namespace opossum {

bool ChunkStatistics::can_prune(const ScanType scan_type, const ColumnID column_id, const AllTypeVariant& value) const {
  return _statistics[column_id]->can_prune(value, scan_type);
}

} // namespace opossum
