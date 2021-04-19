#pragma once

#include "compact_vector.hpp"

namespace opossum {

template <typename T>
using pmr_compact_vector = compact::vector<T, 0, uint64_t, PolymorphicAllocator<uint64_t>>;

}  // namespace opossum
