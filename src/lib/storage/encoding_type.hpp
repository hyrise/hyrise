#pragma once

#include <array>
#include <cstdint>

#include <boost/version.hpp>
#if BOOST_VERSION < 107100                 // TODO(anyone): remove this block once Ubuntu ships boost 1.71
#include "utils/boost_curry_override.hpp"  // NOLINT
#endif
#include <boost/hana/at_key.hpp>
#include <boost/hana/contains.hpp>
#include <boost/hana/equal.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "all_type_variant.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace hana = boost::hana;

enum class EncodingType : uint8_t { Unencoded, Dictionary, RunLength, FixedStringDictionary, FrameOfReference, LZ4 };

inline static std::vector<EncodingType> encoding_type_enum_values{
    EncodingType::Unencoded,        EncodingType::Dictionary,
    EncodingType::RunLength,        EncodingType::FixedStringDictionary,
    EncodingType::FrameOfReference, EncodingType::LZ4};

/**
 * @brief Maps each encoding type to its supported data types
 *
 * This map ensures that segment and encoder templates are only
 * instantiated for supported types and not for all data types.
 *
 * Use data_types if the encoding supports all data types.
 */
constexpr auto supported_data_types_for_encoding_type = hana::make_map(
    hana::make_pair(enum_c<EncodingType, EncodingType::Unencoded>, data_types),
    hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, data_types),
    hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, data_types),
    hana::make_pair(enum_c<EncodingType, EncodingType::FixedStringDictionary>, hana::tuple_t<pmr_string>),
    hana::make_pair(enum_c<EncodingType, EncodingType::FrameOfReference>, hana::tuple_t<int32_t>),
    hana::make_pair(enum_c<EncodingType, EncodingType::LZ4>, data_types));

/**
 * @return an integral constant implicitly convertible to bool
 *
 * Hint: Use hana::value() if you want to use the result
 *       in a constant expression such as constexpr-if.
 */
template <typename SegmentEncodingType, typename ColumnDataType>
constexpr auto encoding_supports_data_type(SegmentEncodingType encoding_type, ColumnDataType data_type) {
  return hana::contains(hana::at_key(supported_data_types_for_encoding_type, encoding_type), data_type);
}

// Version for when EncodingType and DataType are only known at runtime
bool encoding_supports_data_type(EncodingType encoding_type, DataType data_type);

struct SegmentEncodingSpec {
  constexpr SegmentEncodingSpec() : encoding_type{EncodingType::Dictionary} {}
  constexpr SegmentEncodingSpec(EncodingType encoding_type_) : encoding_type{encoding_type_} {}  // NOLINT
  constexpr SegmentEncodingSpec(EncodingType encoding_type_, VectorCompressionType vector_compression_type_)
      : encoding_type{encoding_type_}, vector_compression_type{vector_compression_type_} {}

  EncodingType encoding_type;
  std::optional<VectorCompressionType> vector_compression_type;
};

using ChunkEncodingSpec = std::vector<SegmentEncodingSpec>;

inline constexpr std::array all_encoding_types{EncodingType::Unencoded,        EncodingType::Dictionary,
                                               EncodingType::FrameOfReference, EncodingType::FixedStringDictionary,
                                               EncodingType::RunLength,        EncodingType::LZ4};

inline constexpr std::array all_segment_encoding_specs{
    SegmentEncodingSpec{EncodingType::Unencoded},
    SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
    SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128},
    SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::FixedSizeByteAligned},
    SegmentEncodingSpec{EncodingType::FrameOfReference, VectorCompressionType::SimdBp128},
    SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::FixedSizeByteAligned},
    SegmentEncodingSpec{EncodingType::FixedStringDictionary, VectorCompressionType::SimdBp128},
    SegmentEncodingSpec{EncodingType::LZ4, VectorCompressionType::SimdBp128},
    SegmentEncodingSpec{EncodingType::RunLength}};

}  // namespace opossum
