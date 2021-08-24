#include "storage/index/chunk_index_statistics.hpp"

namespace opossum {

bool operator==(const ChunkIndexStatistics& left, const ChunkIndexStatistics& right) {
  return std::tie(left.column_ids, left.name, left.type) == std::tie(right.column_ids, right.name, right.type);
}

}  // namespace opossum
