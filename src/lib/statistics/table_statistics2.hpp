#pragma once

#include <memory>
#include <vector>

#include "cardinality.hpp"

namespace opossum {

class ChunkStatistics2;

class TableStatistics2 {
 public:
  Cardinality row_count() const;

  std::vector<std::shared_ptr<ChunkStatistics2>> chunk_statistics;
};

}  // namespace opossum
