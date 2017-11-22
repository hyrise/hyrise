#pragma once

#include <emmintrin.h>

#include "base_ns_vector.hpp"
#include "types.hpp"

namespace opossum {

class SimdBp128Vector : public BaseNsVector {
 public:
  explicit SimdBp128Vector(pmr_vector<__m128i> vector, size_t size);
  ~SimdBp128Vector() = default;

  size_t size() const final;
  size_t data_size() const final;
  NsType type() const final;

  const pmr_vector<__m128i>& data() const;

 private:
  const pmr_vector<__m128i> _data;
  const size_t _size;
};
}  // namespace opossum
