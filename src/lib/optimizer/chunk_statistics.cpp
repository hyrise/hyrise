#include "chunk_statistics.hpp"

#include <exception>

namespace opossum {

bool ChunkStatistics::can_prune(const ScanType scan_type, const ColumnID column_id, const AllTypeVariant& value) const {
  switch (scan_type) {
    case ScanType::OpGreaterThan:
      return value > _statistics[column_id]->max();
    default: throw std::logic_error("not implemented");
  }
}

} // namespace opossum
