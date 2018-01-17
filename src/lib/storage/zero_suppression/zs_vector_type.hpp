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
 * @brief Vector types implementing zero suppression
 *
 * This enum is not identical to ZsType. It differs
 * because ZsType::FixedSizeByteAligned can yield three
 * different vector types (one, two, or four bytes)
 * depending on the range of the values in the vector.
 */
enum class ZsVectorType : uint8_t {
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
 * Mapping of zero suppression vector types to zero suppression vectors
 *
 * Note: Add your vector class here!
 */
constexpr auto zs_vector_for_type =
    hana::make_map(hana::make_pair(enum_c<ZsVectorType, ZsVectorType::FixedSize4ByteAligned>,
                                   hana::type_c<FixedSizeByteAlignedVector<uint32_t>>),
                   hana::make_pair(enum_c<ZsVectorType, ZsVectorType::FixedSize2ByteAligned>,
                                   hana::type_c<FixedSizeByteAlignedVector<uint16_t>>),
                   hana::make_pair(enum_c<ZsVectorType, ZsVectorType::FixedSize1ByteAligned>,
                                   hana::type_c<FixedSizeByteAlignedVector<uint8_t>>),
                   hana::make_pair(enum_c<ZsVectorType, ZsVectorType::SimdBp128>, hana::type_c<SimdBp128Vector>));

template <typename ZsVectorT>
ZsVectorType get_zs_vector_type() {
  auto zs_type = ZsVectorType::Invalid;

  hana::fold(zs_vector_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && (hana::second(pair) == hana::type_c<ZsVectorT>)) {
      zs_type = hana::value(hana::first(pair));
      return true;
    }

    return match_found;
  });

  return zs_type;
}

}  // namespace opossum
