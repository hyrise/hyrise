#pragma once

#include "compact_vector.hpp"
#include "types.hpp"

namespace opossum {

// right now, only uint32_t is used as value type
template <typename T>
using pmr_compact_vector = compact::vector<T, 0u, uint64_t, PolymorphicAllocator<uint64_t>>;

}  // namespace opossum
