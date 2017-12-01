#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>

/**
 * Note: Include your null suppression encoder file here!
 */
#include "fixed_size_byte_aligned_encoder.hpp"
#include "simd_bp128_encoder.hpp"

#include "ns_type.hpp"

namespace opossum {

/**
 * Mapping of null suppression types to null suppression encoders
 *
 * Note: Add your encoder class here!
 */
constexpr auto ns_encoder_for_type = hana::make_tuple(
  hana::make_pair(NsType::FixedSize4ByteAligned, hana::type_c<FixedSizeByteAlignedEncoder<uint32_t>>),
  hana::make_pair(NsType::FixedSize2ByteAligned, hana::type_c<FixedSizeByteAlignedEncoder<uint16_t>>),
  hana::make_pair(NsType::FixedSize1ByteAligned, hana::type_c<FixedSizeByteAlignedEncoder<uint8_t>>),
  hana::make_pair(NsType::SimdBp128, hana::type_c<SimdBp128Encoder>));

}  // namespace opossum
