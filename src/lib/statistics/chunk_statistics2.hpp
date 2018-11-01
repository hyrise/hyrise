#pragma once

#include <memory>
#include <vector>

#include "cardinality.hpp"
#include "types.hpp"

namespace opossum {

class BaseSegmentStatistics2;

class ChunkStatistics2 {
 public:
  ChunkStatistics2() = default;
  explicit ChunkStatistics2(const Cardinality row_count);

  Cardinality row_count{0};
  ChunkOffset approx_invalid_row_count{0};
  std::vector<std::shared_ptr<BaseSegmentStatistics2>> segment_statistics;
};

}  // namespace opossum
