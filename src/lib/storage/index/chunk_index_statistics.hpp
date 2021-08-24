#pragma once

#include <string>

#include "chunk_index_type.hpp"
#include "types.hpp"

namespace opossum {

struct ChunkIndexStatistics {
  std::vector<ColumnID> column_ids;
  std::string name;
  ChunkIndexType type;
};

// For googletest
bool operator==(const ChunkIndexStatistics& left, const ChunkIndexStatistics& right);

}  // namespace opossum
