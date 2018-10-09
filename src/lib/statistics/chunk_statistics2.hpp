#pragma once

#include <memory>
#include <vector>

#include "cardinality.hpp"

namespace opossum {

class SegmentStatistics2;

class ChunkStatistics2 {
 public:
  Cardinality row_count{0};
  std::vector<std::shared_ptr<SegmentStatistics2>> segment_statistics;
};

}  // namespace opossum
