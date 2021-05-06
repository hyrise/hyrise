#pragma once

#include "compact_vector.hpp"
#include "types.hpp"

namespace opossum {

// Right now, compact_vector is only used in the BitpackingVector where the value type uint32_t is sufficient.
using pmr_compact_vector = compact::vector<uint32_t, 0u, uint64_t, PolymorphicAllocator<uint64_t>>;

}  // namespace opossum
