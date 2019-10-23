#pragma once

#include <boost/version.hpp>
#if BOOST_VERSION < 107100                 // TODO(anyone): remove this block once Ubuntu ships boost 1.71
#include "utils/boost_curry_override.hpp"  // NOLINT
#endif

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
 * @brief Class types of compressed vectors
 *
 * This enum is not identical to VectorCompressionType. It differs
 * because VectorCompressionType::FixedSizeByteAligned can yield three
 * different vector types (one, two, or four bytes)
 * depending on the range of the values in the vector.
 */
enum class CompressedVectorType : uint8_t {
  FixedSize4ByteAligned,  // uncompressed
  FixedSize2ByteAligned,
  FixedSize1ByteAligned,
  SimdBp128
};

template <typename T>
class FixedSizeByteAlignedVector;
class SimdBp128Vector;

/**
 * Mapping of compressed vector types to compressed vectors
 *
 * Note: Add your vector class here!
 */
constexpr auto compressed_vector_for_type = hana::make_map(
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedSize4ByteAligned>,
                    hana::type_c<FixedSizeByteAlignedVector<uint32_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedSize2ByteAligned>,
                    hana::type_c<FixedSizeByteAlignedVector<uint16_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedSize1ByteAligned>,
                    hana::type_c<FixedSizeByteAlignedVector<uint8_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::SimdBp128>, hana::type_c<SimdBp128Vector>));

/**
 * @brief Returns the CompressedVectorType of a given compressed vector
 *
 * Effectively a reverse lookup in compressed_vector_for_type
 */
template <typename CompressedVectorT>
CompressedVectorType get_compressed_vector_type() {
  auto compression_type = std::optional<CompressedVectorType>{};

  hana::fold(compressed_vector_for_type, false, [&](auto match_found, auto pair) {
    if (!match_found && (hana::second(pair) == hana::type_c<CompressedVectorT>)) {
      compression_type = hana::value(hana::first(pair));
      return true;
    }

    return match_found;
  });

  Assert(compression_type, "CompressedVectorType not added to compressed_vector_for_type");
  return *compression_type;
}

}  // namespace opossum
