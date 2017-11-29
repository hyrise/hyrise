#pragma once

#include "base_ns_decoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedDecoder : BaseNsDecoder {
 public:
  using Vector = FixedSizeByteAlignedVector<UnsignedIntType>;

 public:
  explicit FixedSizeByteAlignedDecoder(const Vector& vector) : _vector{vector} {}

  uint32_t get(size_t i) final { return _vector.data()[i]; }
  size_t size() const final { return _vector.size(); }

 private:
  const Vector& _vector;
};

}  // namespace opossum
