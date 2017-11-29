#pragma once

#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/fold.hpp>
#include <boost/hana/equal.hpp>

#include <cstdint>

namespace opossum {

namespace hana = boost::hana;

template <typename T>
class FixedSizeByteAlignedVector;
class SimdBp128Vector;

enum class NsType : uint8_t {
  Invalid,
  FixedSize32ByteAligned,  // “uncompressed”
  FixedSize16ByteAligned,
  FixedSize8ByteAligned,
  SimdBp128
};

constexpr auto ns_vector_type_pair = hana::make_tuple(
  hana::make_pair(hana::type_c<FixedSizeByteAlignedVector<uint32_t>>, NsType::FixedSize32ByteAligned),
  hana::make_pair(hana::type_c<FixedSizeByteAlignedVector<uint16_t>>, NsType::FixedSize16ByteAligned),
  hana::make_pair(hana::type_c<FixedSizeByteAlignedVector<uint8_t>>, NsType::FixedSize8ByteAligned),
  hana::make_pair(hana::type_c<SimdBp128Vector>, NsType::SimdBp128));

template <typename NsVectorType>
NsType get_ns_type() {
  return hana::fold(ns_vector_type_pair, NsType::Invalid, [](auto ns_type, auto ns_pair) {
    if ((ns_type == NsType::Invalid) && (hana::first(ns_pair) == hana::type_c<NsVectorType>)) {
      return hana::second(ns_pair);
    }

    return ns_type;
  });
}

}  // namespace opossum
