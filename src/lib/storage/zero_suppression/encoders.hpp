#pragma once

#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>

// Include your zero suppression encoder file here!
#include "fixed_size_byte_aligned_encoder.hpp"
#include "simd_bp128_encoder.hpp"

#include "zs_type.hpp"

#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * Mapping of zero suppression types to zero suppression encoders
 *
 * Note: Add your encoder class here!
 */
constexpr auto zs_encoder_for_type = hana::make_map(
    hana::make_pair(enum_c<ZsType, ZsType::FixedSizeByteAligned>, hana::type_c<FixedSizeByteAlignedEncoder>),
    hana::make_pair(enum_c<ZsType, ZsType::SimdBp128>, hana::type_c<SimdBp128Encoder>));

}  // namespace opossum
