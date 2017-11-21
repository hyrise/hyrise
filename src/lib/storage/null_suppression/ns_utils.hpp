#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "fixed_size_byte_aligned_decoder.hpp"
#include "simd_bp128_decoder.hpp"

#include "utils/assert.hpp"


namespace opossum {

class BaseNsEncoder;
class BaseNsVector;

std::unique_ptr<BaseNsEncoder> create_encoder_for_ns_type(NsType type);
std::unique_ptr<BaseNsVector> encode_by_ns_type(NsType type, const std::vector<uint32_t>& vector);

template <typename Functor>
void with_ns_decoder(const BaseNsVector& vector, const Functor& func) {
  switch (vector.type()) {
    case NsType::FixedSize32ByteAligned:
      func(FixedSizeByteAlignedDecoder<uint32_t>{static_cast<const FixedSizeByteAlignedVector<uint32_t>&>(vector)});
      break;
    case NsType::FixedSize16ByteAligned:
      func(FixedSizeByteAlignedDecoder<uint16_t>{static_cast<const FixedSizeByteAlignedVector<uint16_t>&>(vector)});
      break;
    case NsType::FixedSize8ByteAligned:
      func(FixedSizeByteAlignedDecoder<uint8_t>{static_cast<const FixedSizeByteAlignedVector<uint8_t>&>(vector)});
      break;
    case NsType::SimdBp128:
      func(SimdBp128Decoder{static_cast<const SimdBp128Vector&>(vector)});
    default:
      Fail("Unrecognized NsType encountered.");
  }
}

}  // namespace opossum
