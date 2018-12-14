#pragma once

#include <boost/hana/fold.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>
#include <boost/hana/value.hpp>

#include <memory>

// Include your encoded segment file here!
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/run_length_segment.hpp"

#include "storage/encoding_type.hpp"

#include "utils/enum_constant.hpp"
#include "utils/skip_test.hpp"
#include "utils/template_type.hpp"

namespace opossum {

namespace hana = boost::hana;

/**
 * @brief Mapping of encoding types to segments
 *
 * Note: Add your encoded segment class here!
 */
constexpr auto encoded_segment_for_type = hana::make_map(
    hana::make_pair(enum_c<EncodingType, EncodingType::Dictionary>, template_c<DictionarySegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::RunLength>, template_c<RunLengthSegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::FixedStringDictionary>,
                    template_c<FixedStringDictionarySegment>),
    hana::make_pair(enum_c<EncodingType, EncodingType::FrameOfReference>, template_c<FrameOfReferenceSegment>));

/**
 * @brief Resolves the type of an encoded segment.
 *
 * Since encoded segments are immutable, the function accepts a constant reference.
 *
 * @see resolve_segment_type in resolve_type.hpp for info on usage
 */
template <typename ColumnDataType, typename Functor>
void resolve_encoded_segment_type(const BaseEncodedSegment& segment, const Functor& functor) {
  // Iterate over all pairs in the map
  hana::fold(encoded_segment_for_type, false, [&](auto match_found, auto encoded_segment_pair) {
    const auto encoding_type_c = hana::first(encoded_segment_pair);
    const auto segment_template_c = hana::second(encoded_segment_pair);

    constexpr auto encoding_type = hana::value(encoding_type_c);

    // If the segment's encoding type matches that of the pair, we have found the segment's type
    if (!match_found && (encoding_type == segment.encoding_type())) {
      // In limited mode, only build dictionary encoding
      if constexpr (HYRISE_LIMITED_ENCODINGS && encoding_type != EncodingType::Dictionary) {
        try {
          skip_test();
        } catch (std::runtime_error) {
          Fail("Hyrise was built with limited encoding support. The requested encoding is not available");
        }
      } else {
        // Check if ColumnDataType is supported by encoding
        const auto data_type_supported = encoding_supports_data_type(encoding_type_c, hana::type_c<ColumnDataType>);

        // Compile only if ColumnDataType is supported
        if constexpr (hana::value(data_type_supported)) {
          using SegmentTemplateType = typename decltype(segment_template_c)::type;
          using SegmentType = typename SegmentTemplateType::template _template<ColumnDataType>;
          functor(static_cast<const SegmentType&>(segment));
        }
      }
      return true;
    }

    return match_found;
  });
}

}  // namespace opossum
