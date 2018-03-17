#include "clear_cache.hpp"

#include <vector>

namespace opossum {

uint32_t clear_cache() {
  static const auto l3_cache_size_in_byte = 30 * 1024 * 1024;
  static const auto a_bit_more_to_be_safe = static_cast<int>(l3_cache_size_in_byte * 1.1f);

  auto sum = uint32_t{0u};

  auto clear_vector = std::vector<uint32_t>();
  clear_vector.resize(a_bit_more_to_be_safe / sizeof(uint32_t), 13);
  for (auto& x : clear_vector) {
    sum += x;
  }
  clear_vector.resize(0);

  return sum;
}

}  // namespace opossum
