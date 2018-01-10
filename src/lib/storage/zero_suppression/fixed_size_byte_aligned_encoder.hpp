#pragma once

#include "base_zero_suppression_encoder.hpp"
#include "fixed_size_byte_aligned_vector.hpp"

#include "types.hpp"

namespace opossum {

template <typename UnsignedIntType>
class FixedSizeByteAlignedEncoder : public BaseZeroSuppressionEncoder {
 public:
  std::unique_ptr<BaseZeroSuppressionVector> encode(const pmr_vector<uint32_t>& vector,
                                                    const PolymorphicAllocator<size_t>& alloc) final;
};

template <typename UnsignedIntType>
std::unique_ptr<BaseZeroSuppressionVector> FixedSizeByteAlignedEncoder<UnsignedIntType>::encode(
    const pmr_vector<uint32_t>& vector, const PolymorphicAllocator<size_t>& alloc) {
  auto data = pmr_vector<UnsignedIntType>(alloc);
  data.reserve(vector.size());

  for (auto value : vector) {
    data.push_back(static_cast<UnsignedIntType>(value));
  }

  return std::make_unique<FixedSizeByteAlignedVector<UnsignedIntType>>(std::move(data));
}

}  // namespace opossum
