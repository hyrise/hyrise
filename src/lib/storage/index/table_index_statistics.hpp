#pragma once

#include "storage/chunk.hpp"
#include "types.hpp"

namespace hyrise {

struct TableIndexStatistics {
  // TODO(anyone): Simplify TableIndexStatistics when chunk-wise indexing is removed.
  std::vector<ColumnID> column_ids;
  std::vector<std::pair<ChunkID, std::shared_ptr<Chunk>>> chunk_ids;
};

// For googletest
bool operator==(const TableIndexStatistics& left, const TableIndexStatistics& right);

}  // namespace hyrise
