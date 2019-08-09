#include "storage/index/index_statistics.hpp"

namespace opossum {

bool operator==(const IndexStatistics& left, const IndexStatistics& right) {
  return std::tie(left.column_ids, left.name, left.type) == std::tie(right.column_ids, right.name, right.type);
}

}  // namespace opossum
