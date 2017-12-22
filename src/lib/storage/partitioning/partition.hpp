#pragma once

#include <all_type_variant.hpp>
#include <types.hpp>

namespace opossum {

class Partition {
  std::vector<ChunkID> chunks;
};

}  // namespace opossum
