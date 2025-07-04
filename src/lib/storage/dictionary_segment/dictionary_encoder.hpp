#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>

#include "storage/base_segment_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace hyrise {

/**
 * @brief Encodes a segment using dictionary encoding and compresses its attribute vector using vector compression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it
 * using fixed-width integer encoding.
 */
template <auto Encoding>
class DictionaryEncoder : public SegmentEncoder<DictionaryEncoder<Encoding>> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, Encoding>;
  static constexpr auto _uses_vector_compression = true;  // see base_segment_encoder.hpp for details

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    // Vectors to gather the input segment's data. This data is used in a later step to construct the actual dictionary
    // and attribute vector.
    auto dense_values = std::vector<T>{};    // Contains the actual values (no NULLs).
    auto null_values = std::vector<bool>{};  // Bitmap to mark NULL values.

    auto max_string_length = size_t{0};

    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      const auto segment_size = std::distance(segment_it, segment_end);
      dense_values.reserve(segment_size);  // Potentially overallocate for segments with NULLs
      null_values.resize(segment_size);    // Resized to size of segment.

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        if (!segment_it->is_null()) {
          const auto segment_value = segment_it->value();
          dense_values.push_back(segment_value);

          if constexpr (Encoding == EncodingType::FixedStringDictionary) {
            if (segment_value.size() > max_string_length) {
              max_string_length = segment_value.size();
            }
          }
        } else {
          null_values[current_position] = true;
        }
      }
    });

    auto dictionary = pmr_vector<T>(dense_values.cbegin(), dense_values.cend(), allocator);
    boost::sort::pdqsort(dictionary.begin(), dictionary.end());
    dictionary.erase(std::unique(dictionary.begin(), dictionary.end()), dictionary.cend());
    dictionary.shrink_to_fit();

    const auto null_value_id = static_cast<uint32_t>(dictionary.size());

    auto uncompressed_attribute_vector = pmr_vector<uint32_t>{null_values.size(), allocator};
    auto values_iter = dense_values.cbegin();
    const auto null_values_size = null_values.size();
    for (auto current_position = size_t{0}; current_position < null_values_size; ++current_position) {
      if (!null_values[current_position]) {
        const auto value_id = _get_value_id(dictionary, *values_iter);
        uncompressed_attribute_vector[current_position] = value_id;
        ++values_iter;
      } else {
        uncompressed_attribute_vector[current_position] = null_value_id;
      }
    }

    // While the highest value ID used for a value is (dictionary.size() - 1), we need to account for NULL values,
    // encoded as (dictionary.size()). Thus, the highest value id seen in the attribute vector is the one encoding
    // NULL.
    const auto max_value_id = null_value_id;

    auto compressed_attribute_vector = compress_vector(
        uncompressed_attribute_vector, SegmentEncoder<DictionaryEncoder<Encoding>>::vector_compression_type(),
        allocator, {max_value_id});

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      // Encode a segment with a FixedStringVector as dictionary. pmr_string is the only supported type.
      return std::make_shared<FixedStringDictionarySegment<T>>(
          FixedStringVector(dictionary.cbegin(), dictionary.cend(), max_string_length, allocator),
          std::move(compressed_attribute_vector));
    } else {
      // Encode a segment with a pmr_vector<T> as dictionary.
      return std::make_shared<DictionarySegment<T>>(std::move(dictionary), std::move(compressed_attribute_vector));
    }
  }

 private:
  template <typename U, typename T>
  static ValueID _get_value_id(const U& dictionary, const T& value) {
    return ValueID{static_cast<ValueID::base_type>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)))};
  }
};

}  // namespace hyrise
