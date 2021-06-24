#pragma once

#include <string>

#include "index_statistics.hpp"
#include "segment_index_type.hpp"
#include "types.hpp"

namespace opossum {

struct PartialIndexStatistics : IndexStatistics {
  std::vector<ChunkID> chunk_ids;
};

// For googletest
bool operator==(const PartialIndexStatistics& left, const PartialIndexStatistics& right);

}  // namespace opossum
