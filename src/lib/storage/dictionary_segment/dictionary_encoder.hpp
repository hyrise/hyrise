#pragma once

#include <algorithm>
#include <limits>
#include <memory>

#include "storage/base_segment_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * @brief Encodes a segment using dictionary encoding and compresses its attribute vector using vector compression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it
 * using fixed-size byte-aligned encoding.
 */
template <auto Encoding>
class DictionaryEncoder : public SegmentEncoder<DictionaryEncoder<Encoding>> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, Encoding>;
  static constexpr auto _uses_vector_compression = true;  // see base_segment_encoder.hpp for details

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                 const PolymorphicAllocator<T>& allocator) {
    // Vectors to gather the input segment's data. This data is used in a later step to
    // construct the actual dictionary and attribute vector.
    std::vector<T> dense_values;    // contains the actual values (no NULLs)
    std::vector<bool> null_values;  // bitmap to mark NULL values

    auto max_string_length = size_t{0};

    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      const auto segment_size = std::distance(segment_it, segment_end);
      dense_values.reserve(segment_size);  // potentially overallocate for segments with NULLs
      null_values.resize(segment_size);    // resized to size of segment

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto segment_value = segment_item.value();
          dense_values.push_back(segment_value);

          if constexpr (Encoding == EncodingType::FixedStringDictionary) {
            if (segment_value.size() > max_string_length) max_string_length = segment_value.size();
          }
        } else {
          null_values[current_position] = true;
        }
      }
    });

    auto dictionary = std::make_shared<pmr_vector<T>>(dense_values.cbegin(), dense_values.cend(), allocator);
    std::sort(dictionary->begin(), dictionary->end());
    dictionary->erase(std::unique(dictionary->begin(), dictionary->end()), dictionary->cend());
    dictionary->shrink_to_fit();

    const auto null_value_id = static_cast<uint32_t>(dictionary->size());

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      // Encode a segment with a FixedStringVector as dictionary. pmr_string is the only supported type
      auto fixed_string_dictionary =
          std::make_shared<FixedStringVector>(dictionary->cbegin(), dictionary->cend(), max_string_length);
      const auto compressed_attribute_vector = _create_compressed_attribute_vector(
          fixed_string_dictionary, dense_values, null_values, allocator, null_value_id);
      return std::allocate_shared<FixedStringDictionarySegment<T>>(allocator, fixed_string_dictionary,
                                                                   compressed_attribute_vector, ValueID{null_value_id});
    } else {
      // Encode a segment with a pmr_vector<T> as dictionary
      const auto compressed_attribute_vector =
          _create_compressed_attribute_vector(dictionary, dense_values, null_values, allocator, null_value_id);
      return std::allocate_shared<DictionarySegment<T>>(allocator, dictionary, compressed_attribute_vector,
                                                        ValueID{null_value_id});
    }
  }

 private:
  template <typename U, typename T>
  static ValueID _get_value_id(const U& dictionary, const T& value) {
    return ValueID{static_cast<ValueID::base_type>(
        std::distance(dictionary->cbegin(), std::lower_bound(dictionary->cbegin(), dictionary->cend(), value)))};
  }

  template <typename U, typename T>
  const std::shared_ptr<const BaseCompressedVector> _create_compressed_attribute_vector(
      const U& dictionary, const std::vector<T>& values, const std::vector<bool>& null_values,
      const PolymorphicAllocator<T>& allocator, const uint32_t null_value_id) {
    auto attribute_vector = pmr_vector<uint32_t>{allocator};
    attribute_vector.resize(null_values.size());

    auto values_iter = values.cbegin();
    for (auto current_position = size_t{0}; current_position < null_values.size(); ++current_position) {
      if (!null_values[current_position]) {
        const auto value_id = _get_value_id(dictionary, *values_iter);
        attribute_vector[current_position] = value_id;
        ++values_iter;
      } else {
        attribute_vector[current_position] = null_value_id;
      }
    }

    // The dictionary size is incremented here to create the value ID for possible null values.
    const auto max_value_id = dictionary->size() + 1u;

    return std::shared_ptr<const BaseCompressedVector>(
        compress_vector(attribute_vector, SegmentEncoder<DictionaryEncoder<Encoding>>::vector_compression_type(),
                        allocator, {max_value_id}));
  }
};

}  // namespace opossum
