#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/value.hpp>

#include <memory>

// Include your encoded column file here!
#include "storage/dictionary_column.hpp"
#include "storage/fixed_string_dictionary_column.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/run_length_segment.hpp"

#include "storage/encoding_type.hpp"

#include "utils/enum_constant.hpp"
#include "utils/template_type.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Mapping of encoding types to columns
 *
 * Note: Add your encoded column class here!
 */
constexpr auto encoded_segment_for_type = hana::make_map(
    hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, template_c<DictionarySegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, template_c<RunLengthSegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::FixedStringDictionary>, template_c<FixedStringDictionarySegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::FrameOfReference>, template_c<FrameOfReferenceSegment>));

/**
 * @brief Resolves the type of an encoded column.
 *
 * Since encoded columns are immutable, the function accepts a constant reference.
 *
 * @see resolve_cxlumn_type in resolve_type.hpp for info on usage
 */
template <typename CxlumnDataType, typename Functor>
void resolve_encoded_segment_type(const BaseEncodedSegment& column, const Functor& functor) {
  // Iterate over all pairs in the map
  hana::fold(encoded_segment_for_type, false, [&](auto match_found, auto encoded_segment_pair) {
    const auto encoding_type_c = hana::first(encoded_segment_pair);
    const auto column_template_c = hana::second(encoded_segment_pair);

    constexpr auto encoding_type = hana::value(encoding_type_c);

    // If the column’s encoding type matches that of the pair, we have found the column’s type
    if (!match_found && (encoding_type == column.encoding_type())) {
      // Check if CxlumnDataType is supported by encoding
      const auto data_type_supported = encoding_supports_data_type(encoding_type_c, hana::type_c<CxlumnDataType>);

      // clang-format off

      // Compile only if CxlumnDataType is supported
      if constexpr(hana::value(data_type_supported)) {
        using ColumnTemplateType = typename decltype(column_template_c)::type;
        using ColumnType = typename ColumnTemplateType::template _template<CxlumnDataType>;
        functor(static_cast<const ColumnType&>(column));
      }

      // clang-format on

      return true;
    }

    return match_found;
  });
}

}  // namespace opossum
