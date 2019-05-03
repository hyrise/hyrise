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
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    // See: https://goo.gl/MCM5rr
    // Create dictionary (enforce uniqueness and sorting)
    const auto& values = value_segment->values();

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      // Encode a segment with a FixedStringVector as dictionary. pmr_string is the only supported type
      return _encode_dictionary_segment(
          FixedStringVector{values.cbegin(), values.cend(), _calculate_fixed_string_length(values), values.size()},
          value_segment);
    } else {
      // Encode a segment with a pmr_vector<T> as dictionary
      return _encode_dictionary_segment(pmr_vector<T>{values.cbegin(), values.cend(), values.get_allocator()},
                                        value_segment);
    }
  }

  template <typename T>
  static std::enable_if_t<std::is_arithmetic_v<T>, pmr_vector<T>> create_dictionary(const std::vector<T>& values, const PolymorphicAllocator<T>& allocator) {
    std::vector<T> values_sorted(values.cbegin(), values.cend());
    std::sort(values_sorted.begin(), values_sorted.end());
    pmr_vector<T> dictionary(allocator);
    dictionary.reserve(values.size());
    std::unique_copy(std::make_move_iterator(values_sorted.cbegin()), std::make_move_iterator(values_sorted.cend()), std::back_inserter(dictionary));

    return dictionary;
  }

  template <typename T>
  static std::enable_if_t<!std::is_arithmetic_v<T>, pmr_vector<T>> create_dictionary(const std::vector<T>& values, const PolymorphicAllocator<T>& allocator) {
    std::vector<T> values_sorted(values.cbegin(), values.cend());
    std::sort(values_sorted.begin(), values_sorted.end());
    pmr_vector<T> dictionary(allocator);
    dictionary.reserve(values.size());
    std::unique_copy(std::make_move_iterator(values_sorted.cbegin()), std::make_move_iterator(values_sorted.cend()), std::back_inserter(dictionary));

    return dictionary;
  }

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable, const PolymorphicAllocator<T>& allocator) {
    std::vector<T> values;
    std::vector<bool> null_values;

    auto max_string_length = size_t{0};
    std::unordered_set<T> unique_values;

    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      const auto segment_size = std::distance(segment_it, segment_end);
      values.reserve(segment_size);
      null_values.resize(segment_size);

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto segment_value =  segment_item.value();
          values.push_back(segment_value);
          unique_values.insert(segment_value);

          if constexpr (Encoding == EncodingType::FixedStringDictionary) {
            if (segment_value.size() > max_string_length) max_string_length = segment_value.size();
          }
        } else {
          null_values[current_position] = true;
        }
      }
    });

    pmr_vector<T> dictionary(unique_values.cbegin(), unique_values.cend(), allocator);

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      // Encode a segment with a FixedStringVector as dictionary. pmr_string is the only supported type
      return _encode_dictionary_segment(
          FixedStringVector{dictionary.cbegin(), dictionary.cend(), max_string_length, dictionary.size()},
          values, null_values, allocator);
    } else {
      // Encode a segment with a pmr_vector<T> as dictionary
      return _encode_dictionary_segment(dictionary, values, null_values, allocator);
    }
  }

 private:
  template <typename U, typename T>
  static ValueID _get_value_id(const U& dictionary, const T& value) {
    return ValueID{static_cast<ValueID::base_type>(
        std::distance(dictionary.cbegin(), std::lower_bound(dictionary.cbegin(), dictionary.cend(), value)))};
  }

  template <typename U, typename T>
  std::shared_ptr<BaseEncodedSegment> _encode_dictionary_segment(const U& dictionary, const std::vector<T>& values,
      const std::vector<bool>& null_values, const PolymorphicAllocator<T>& allocator) {
    const auto null_value_id = static_cast<uint32_t>(dictionary.size());
    auto attribute_vector = pmr_vector<uint32_t>{allocator};
    attribute_vector.reserve(null_values.size());

    auto values_iter = values.cbegin();
    for (auto current_position = size_t{0}; current_position < null_values.size(); ++current_position) {
      if (!null_values[current_position]) {
        const auto value_id = _get_value_id(dictionary, *values_iter);
        attribute_vector.push_back(value_id);
        ++values_iter;
      } else {
        attribute_vector.push_back(null_value_id);
      }
    }

    // We need to increment the dictionary size here because of possible null values.
    const auto max_value = dictionary.size() + 1u;

    auto encoded_attribute_vector = compress_vector(
        attribute_vector, SegmentEncoder<DictionaryEncoder<Encoding>>::vector_compression_type(), allocator, {max_value});
    auto dictionary_sptr = std::allocate_shared<U>(allocator, std::move(dictionary));
    auto attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_attribute_vector));

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      return std::allocate_shared<FixedStringDictionarySegment<T>>(allocator, dictionary_sptr, attribute_vector_sptr,
                                                                   ValueID{null_value_id});
    } else {
      return std::allocate_shared<DictionarySegment<T>>(allocator, dictionary_sptr, attribute_vector_sptr,
                                                        ValueID{null_value_id});
    }
  }
  template <typename U, typename T>
  std::shared_ptr<BaseEncodedSegment> _encode_dictionary_segment(
      U dictionary, const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto& values = value_segment->values();
    const auto alloc = values.get_allocator();

    // Remove null values from value vector
    if (value_segment->is_nullable()) {
      const auto& null_values = value_segment->null_values();

      // Swap values to back if value is null
      auto erase_from_here_it = dictionary.end();
      auto null_it = null_values.crbegin();
      for (auto dict_it = dictionary.rbegin(); dict_it != dictionary.rend(); ++dict_it, ++null_it) {
        if (*null_it) {
          std::iter_swap(dict_it, --erase_from_here_it);
        }
      }

      // Erase null values
      dictionary.erase(erase_from_here_it, dictionary.end());
    }

    std::sort(dictionary.begin(), dictionary.end());
    dictionary.erase(std::unique(dictionary.begin(), dictionary.end()), dictionary.end());
    dictionary.shrink_to_fit();

    auto attribute_vector = pmr_vector<uint32_t>{values.get_allocator()};
    attribute_vector.reserve(values.size());

    const auto null_value_id = static_cast<uint32_t>(dictionary.size());

    if (value_segment->is_nullable()) {
      const auto& null_values = value_segment->null_values();

      /**
       * Iterators are used because values and null_values are of
       * type tbb::concurrent_vector and thus index-based access isn’t in O(1)
       */
      auto value_it = values.cbegin();
      auto null_value_it = null_values.cbegin();
      for (; value_it != values.cend(); ++value_it, ++null_value_it) {
        if (*null_value_it) {
          attribute_vector.push_back(null_value_id);
          continue;
        }

        const auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector.push_back(value_id);
      }
    } else {
      auto value_it = values.cbegin();
      for (; value_it != values.cend(); ++value_it) {
        const auto value_id = _get_value_id(dictionary, *value_it);
        attribute_vector.push_back(value_id);
      }
    }

    // We need to increment the dictionary size here because of possible null values.
    const auto max_value = dictionary.size() + 1u;

    auto encoded_attribute_vector = compress_vector(
        attribute_vector, SegmentEncoder<DictionaryEncoder<Encoding>>::vector_compression_type(), alloc, {max_value});
    auto dictionary_sptr = std::allocate_shared<U>(alloc, std::move(dictionary));
    auto attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(encoded_attribute_vector));

    if constexpr (Encoding == EncodingType::FixedStringDictionary) {
      return std::allocate_shared<FixedStringDictionarySegment<T>>(alloc, dictionary_sptr, attribute_vector_sptr,
                                                                   ValueID{null_value_id});
    } else {
      return std::allocate_shared<DictionarySegment<T>>(alloc, dictionary_sptr, attribute_vector_sptr,
                                                        ValueID{null_value_id});
    }
  }

  size_t _calculate_fixed_string_length(const pmr_concurrent_vector<pmr_string>& values) const {
    size_t max_string_length = 0;
    for (const auto& value : values) {
      if (value.size() > max_string_length) max_string_length = value.size();
    }
    return max_string_length;
  }
};

}  // namespace opossum
