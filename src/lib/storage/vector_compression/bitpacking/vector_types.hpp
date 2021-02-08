#pragma once

#include "compact_vector.hpp"

namespace opossum {

template <typename T, unsigned B>
using pmr_bitpacking_vector = compact::vector<T, B, uint64_t, PolymorphicAllocator<uint64_t>>;

}  // namespace opossum