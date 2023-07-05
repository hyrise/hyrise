#pragma once

#include <algorithm>
#include <limits>
#include <memory>

#include "storage/base_segment_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/variable_string_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace hyrise {

/**
 * @brief Encodes a segment using variable string dictionary encoding and compresses its attribute vector using vector compression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it
 * using fixed-width integer encoding.
 */
class VariableStringDictionaryEncoder : public SegmentEncoder<VariableStringDictionaryEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::VariableStringDictionary>;
  static constexpr auto _uses_vector_compression = true;  // see base_segment_encoder.hpp for details

  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<pmr_string> segment_iterable,
                                                     const PolymorphicAllocator<pmr_string>& allocator) {
    // Vectors to gather the input segment's data. This data is used in a later step to
    // construct the actual dictionary and attribute vector.
    auto dense_values = std::vector<pmr_string>();  // contains the actual values (no NULLs)
    auto null_values = std::vector<bool>();         // bitmap to mark NULL values
    auto string_to_chunk_offsets = std::unordered_map<pmr_string, std::vector<ChunkOffset>>();
    auto segment_size = uint32_t{0};

    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      segment_size = std::distance(segment_it, segment_end);
      dense_values.reserve(segment_size);  // potentially overallocate for segments with NULLs
      null_values.resize(segment_size);    // resized to size of segment

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto& segment_value = segment_item.value();
          dense_values.push_back(segment_value);
          string_to_chunk_offsets[segment_value].push_back(ChunkOffset(current_position));
        } else {
          null_values[current_position] = true;
        }
      }
    });

    // Eliminate duplicate strings.
    std::sort(dense_values.begin(), dense_values.end());
    dense_values.erase(std::unique(dense_values.begin(), dense_values.end()), dense_values.cend());
    dense_values.shrink_to_fit();

    // Compute total compressed data size.
    auto total_size = uint32_t{0};
    for (const auto& value : dense_values) {
      total_size += value.size() + 1;
    }

    // TODO::(us) reserve instead of resize, beware null terminators
    auto klotz = std::make_shared<pmr_vector<char>>(pmr_vector<char>(total_size));
    // We assume segment size up to 4 GByte.
    auto string_offsets = std::unordered_map<pmr_string, uint32_t>();
    auto string_value_ids = std::unordered_map<pmr_string, ValueID>();
    auto current_offset = uint32_t{0};
    auto current_value_id = ValueID{0};

    for (const auto& value : dense_values) {
      memcpy(klotz->data() + current_offset, value.c_str(), value.size());
      string_offsets[value] = current_offset;
      string_value_ids[value] = current_value_id++;
      current_offset += value.size() + 1;
    }

    auto chunk_offset_to_value_id = std::make_shared<pmr_vector<uint32_t>>(pmr_vector<uint32_t>(segment_size));
    auto offset_vector = std::make_shared<pmr_vector<uint32_t>>(pmr_vector<uint32_t>(dense_values.size()));
    offset_vector->shrink_to_fit();

    for (const auto& [string, chunk_offsets] : string_to_chunk_offsets) {
      const auto here_offset = string_offsets[string];
      const auto here_value_id = string_value_ids[string];
      (*offset_vector)[here_value_id] = here_offset;
      for (const auto chunk_offset : chunk_offsets) {
        (*chunk_offset_to_value_id)[chunk_offset] = here_value_id;
      }
    }

    const auto null_value_id = dense_values.size();
    for (auto offset = ChunkOffset{0}; offset < segment_size; ++offset) {
      const auto is_null = null_values[offset];
      if (is_null) {
        (*chunk_offset_to_value_id)[offset] = null_value_id;
      }
    }

    const auto max_value_id = current_value_id;
    const auto compressed_chunk_offset_to_value_id = std::shared_ptr<const BaseCompressedVector>(
        compress_vector(*chunk_offset_to_value_id, SegmentEncoder<VariableStringDictionaryEncoder>::vector_compression_type(),
                        allocator, {max_value_id}));

    return std::make_shared<VariableStringDictionarySegment>(klotz, compressed_chunk_offset_to_value_id, offset_vector);
  }

// private:
//  template <typename U, typename T>
//  static ValueID _get_value_id(const U& dictionary, const T& value) {
//    return ValueID{static_cast<ValueID::base_type>(
//        std::distance(dictionary->cbegin(), std::lower_bound(dictionary->cbegin(), dictionary->cend(), value)))};
//  }
};

}  // namespace hyrise
