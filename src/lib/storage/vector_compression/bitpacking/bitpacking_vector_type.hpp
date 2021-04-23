#pragma once

#include "compact_vector.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
using pmr_compact_vector = compact::vector<T, 0u, uint64_t, PolymorphicAllocator<uint64_t>>;

}  // namespace opossum
