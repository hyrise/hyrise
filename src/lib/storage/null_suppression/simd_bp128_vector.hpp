#pragma once

#include <emmintrin.h>

#include "base_encoded_vector.hpp"
#include "types.hpp"

namespace opossum {

class SimdBp128Vector : public BaseEncodedVector {
 public:
  static constexpr auto block_size = 128u;
  static constexpr auto blocks_in_meta_block = 16u;
  static constexpr auto meta_block_size = block_size * blocks_in_meta_block;

 public:
  explicit SimdBp128Vector(pmr_vector<__m128i> vector, size_t size);
  ~SimdBp128Vector() = default;

  uint32_t get(const size_t i) const final;
  size_t size() const final;

  const pmr_vector<__m128i>& data() const;

 private:
  const pmr_vector<__m128i> _data;
  const size_t _size;
};
}  // namespace opossum