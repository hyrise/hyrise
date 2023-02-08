#pragma once

#include <cstdint>
#include <optional>

#include <boost/hana/equal.hpp>
#include <boost/hana/fold.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/value.hpp>

#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace hyrise {

namespace hana = boost::hana;

/**
 * @brief Class types of compressed vectors
 *
 * This enum is not identical to VectorCompressionType. It differs
 * because VectorCompressionType::FixedWidthInteger can yield three
 * different vector types (one, two, or four bytes)
 * depending on the range of the values in the vector.
 */
enum class CompressedVectorType : uint8_t {
  BitPacking,
  FixedWidthInteger1Byte,
  FixedWidthInteger2Byte,
  FixedWidthInteger4Byte,  // uncompressed
};

std::ostream& operator<<(std::ostream& stream, const CompressedVectorType compressed_vector_type);

template <typename T>
class FixedWidthIntegerVector;
class BitPackingVector;

/**
 * Mapping of compressed vector types to compressed vectors
 *
 * Note: Add your vector class here!
 */
constexpr auto compressed_vector_for_type = hana::make_map(
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedWidthInteger4Byte>,
                    hana::type_c<FixedWidthIntegerVector<uint32_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedWidthInteger2Byte>,
                    hana::type_c<FixedWidthIntegerVector<uint16_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::FixedWidthInteger1Byte>,
                    hana::type_c<FixedWidthIntegerVector<uint8_t>>),
    hana::make_pair(enum_c<CompressedVectorType, CompressedVectorType::BitPacking>, hana::type_c<BitPackingVector>));

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

}  // namespace hyrise
