#pragma once

#include <emmintrin.h>

#include "base_ns_vector.hpp"
#include "simd_bp128_iterator.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Vector : public NsVector<SimdBp128Vector> {
 public:
  using ConstIterator = SimdBp128Iterator;

 public:
  explicit SimdBp128Vector(pmr_vector<__m128i> vector, size_t size);
  ~SimdBp128Vector() = default;

  size_t _on_size() const;
  size_t _on_data_size() const;

  std::unique_ptr<SimdBp128Decoder> _on_create_decoder() const;

  ConstIterator _on_cbegin() const;
  ConstIterator _on_cend() const;

 private:
  const pmr_vector<__m128i> _data;
  const size_t _size;
};
}  // namespace opossum
