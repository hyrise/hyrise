#pragma once

#include "base_ns_encoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedEncoder : public BaseNsEncoder {
 public:
  std::unique_ptr<BaseNsVector> encode(const pmr_vector<uint32_t>& vector,
                                       const PolymorphicAllocator<size_t>& alloc) final;

 private:
  pmr_vector<UnsignedIntType> _data;
};

template <typename UnsignedIntType>
std::unique_ptr<BaseNsVector> FixedSizeByteAlignedEncoder<UnsignedIntType>::encode(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc) {
  _data = pmr_vector<UnsignedIntType>{alloc};
  _data.reserve(vector.size());

  for (auto value : vector) {
    _data.push_back(static_cast<UnsignedIntType>(value));
  }

  return std::make_unique<FixedSizeByteAlignedVector<UnsignedIntType>>(std::move(_data));
}

template <>
std::unique_ptr<BaseNsVector> FixedSizeByteAlignedEncoder<uint32_t>::encode(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc) {
  _data = pmr_vector<uint32_t>{vector, alloc};
  return std::make_unique<FixedSizeByteAlignedVector<uint32_t>>(std::move(_data));
}

}  // namespace opossum
