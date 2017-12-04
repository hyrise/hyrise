#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/map.hpp>

// Include your null suppression encoder file here!
#include "fixed_size_byte_aligned_encoder.hpp"
#include "simd_bp128_encoder.hpp"

#include "ns_type.hpp"

#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * Mapping of null suppression types to null suppression encoders
 *
 * Note: Add your encoder class here!
 */
constexpr auto ns_encoder_for_type = hana::make_map(
  hana::make_pair(enum_c<NsType::FixedSize4ByteAligned>, hana::type_c<FixedSizeByteAlignedEncoder<uint32_t>>),
  hana::make_pair(enum_c<NsType::FixedSize2ByteAligned>, hana::type_c<FixedSizeByteAlignedEncoder<uint16_t>>),
  hana::make_pair(enum_c<NsType::FixedSize1ByteAligned>, hana::type_c<FixedSizeByteAlignedEncoder<uint8_t>>),
  hana::make_pair(enum_c<NsType::SimdBp128>, hana::type_c<SimdBp128Encoder>));

}  // namespace opossum
