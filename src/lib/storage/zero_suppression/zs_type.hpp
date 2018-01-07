#pragma once

#include <boost/hana/equal.hpp>
#include <boost/hana/fold.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/value.hpp>

#include <cstdint>

#include "utils/enum_constant.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Implemented Zero Suppression Schemes
 *
 * Zero suppression is also known as Null Suppression
 */
enum class ZsType : uint8_t {
  Invalid,
  FixedSize4ByteAligned,  // “uncompressed”
  FixedSize2ByteAligned,
  FixedSize1ByteAligned,
  SimdBp128
};

template <typename T>
class FixedSizeByteAlignedVector;
class SimdBp128Vector;

/**
 * Mapping of zero suppression types to zero suppression vectors
 *
 * Note: Add your vector class here!
 */
constexpr auto zs_vector_for_type = hana::make_map(
    hana::make_pair(enum_c<ZsType, ZsType::FixedSize4ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint32_t>>),
    hana::make_pair(enum_c<ZsType, ZsType::FixedSize2ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint16_t>>),
    hana::make_pair(enum_c<ZsType, ZsType::FixedSize1ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint8_t>>),
    hana::make_pair(enum_c<ZsType, ZsType::SimdBp128>, hana::type_c<SimdBp128Vector>));

template <typename ZsVectorType>
ZsType get_zs_type() {
  auto zs_type = ZsType::Invalid;

  hana::fold(zs_vector_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && (hana::second(pair) == hana::type_c<ZsVectorType>)) {
      zs_type = hana::value(hana::first(pair));
      return true;
    }

    return match_found;
  });

  return zs_type;
}

}  // namespace opossum
