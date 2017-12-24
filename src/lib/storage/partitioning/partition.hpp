#pragma once

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "types.hpp"

namespace opossum {

class Partition {
  std::vector<Chunk> chunks;
};

}  // namespace opossum
