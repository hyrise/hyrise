#include "storage/index/table_index_statistics.hpp"

namespace hyrise {

bool operator==(const TableIndexStatistics& left, const TableIndexStatistics& right) {
  return std::tie(left.column_ids, left.chunk_ids) == std::tie(right.column_ids, right.chunk_ids);
}

}  // namespace hyrise
