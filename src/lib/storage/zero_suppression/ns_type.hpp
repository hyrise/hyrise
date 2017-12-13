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

enum class NsType : uint8_t {
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
constexpr auto ns_vector_for_type = hana::make_map(
    hana::make_pair(enum_c<NsType, NsType::FixedSize4ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint32_t>>),
    hana::make_pair(enum_c<NsType, NsType::FixedSize2ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint16_t>>),
    hana::make_pair(enum_c<NsType, NsType::FixedSize1ByteAligned>, hana::type_c<FixedSizeByteAlignedVector<uint8_t>>),
    hana::make_pair(enum_c<NsType, NsType::SimdBp128>, hana::type_c<SimdBp128Vector>));

template <typename NsVectorType>
NsType get_ns_type() {
  auto ns_type = NsType::Invalid;

  hana::fold(ns_vector_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && (hana::second(pair) == hana::type_c<NsVectorType>)) {
      ns_type = hana::value(hana::first(pair));
      return true;
    }

    return match_found;
  });

  return ns_type;
}

}  // namespace opossum
