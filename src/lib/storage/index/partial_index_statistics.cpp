#include "partial_index_statistics.hpp"

namespace opossum {

bool operator==(const PartialIndexStatistics& left, const PartialIndexStatistics& right) {
  return std::tie(left.column_ids, left.name, left.type, left.chunk_ids) == std::tie(right.column_ids, right.name, right.type, right.chunk_ids);
}

}  // namespace opossum