#pragma once

#include <string>

#include "storage/chunk.hpp"
#include "storage/index/abstract_table_index.hpp"
#include "types.hpp"

namespace hyrise {

struct TableIndexStatistics {
  std::vector<ColumnID> column_ids;
  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunk_ids;
  TableIndexType type;
};

// For googletest
bool operator==(const TableIndexStatistics& left, const TableIndexStatistics& right);

}  // namespace hyrise
