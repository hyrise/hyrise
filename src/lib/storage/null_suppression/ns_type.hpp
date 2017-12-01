#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/fold.hpp>
#include <boost/hana/equal.hpp>

#include <cstdint>

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
 * Mapping of null suppression types to null suppression vectors
 *
 * Note: Add your vector class here!
 */
constexpr auto ns_type_vector_pair = hana::make_tuple(
  hana::make_pair(NsType::FixedSize4ByteAligned, hana::type_c<FixedSizeByteAlignedVector<uint32_t>>),
  hana::make_pair(NsType::FixedSize2ByteAligned, hana::type_c<FixedSizeByteAlignedVector<uint16_t>>),
  hana::make_pair(NsType::FixedSize1ByteAligned, hana::type_c<FixedSizeByteAlignedVector<uint8_t>>),
  hana::make_pair(NsType::SimdBp128, hana::type_c<SimdBp128Vector>));

template <typename NsVectorType>
NsType get_ns_type() {
  return hana::fold(ns_type_vector_pair, NsType::Invalid, [](auto ns_type, auto ns_pair) {
    if ((ns_type == NsType::Invalid) && (hana::second(ns_pair) == hana::type_c<NsVectorType>)) {
      return hana::first(ns_pair);
    }

    return ns_type;
  });
}

}  // namespace opossum
