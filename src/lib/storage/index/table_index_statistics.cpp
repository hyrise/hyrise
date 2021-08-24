#include "storage/index/table_index_statistics.hpp"

namespace opossum {

bool operator==(const TableIndexStatistics& left, const TableIndexStatistics& right) {
  return std::tie(left.column_ids, left.chunk_ids, left.name, left.type) ==
         std::tie(right.column_ids, right.chunk_ids, right.name, right.type);
}

}  // namespace opossum
