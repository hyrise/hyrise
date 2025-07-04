#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include <boost/sort/sort.hpp>
#include <boost/unordered/unordered_flat_map.hpp>

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
 * @brief Encodes a segment using variable string dictionary encoding and compresses its attribute vector using vector
 * dcompression.
 *
 * The algorithm first creates an attribute vector of standard size (uint32_t) and then compresses it using fixed-width
 * integer encoding.
 */
class VariableStringDictionaryEncoder : public SegmentEncoder<VariableStringDictionaryEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::VariableStringDictionary>;
  static constexpr auto _uses_vector_compression = true;  // See base_segment_encoder.hpp for details.

  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<pmr_string> segment_iterable,
                                                     const PolymorphicAllocator<pmr_string>& allocator) {
    // Vectors to gather the input segment's data. This data is used in a later step to construct the actual dictionary
    // and attribute vector.
    auto dense_values = std::vector<pmr_string>{};  // Contains the actual values (no NULLs).
    auto null_values = std::vector<bool>{};         // Bitmap to mark NULL values.
    // Maps strings to ChunkOffsets for faster writing of vector that maps ChunkOffsets to ValueID.
    auto string_to_chunk_offsets = boost::unordered_flat_map<pmr_string, std::vector<ChunkOffset>>{};
    auto segment_size = size_t{0};

    // Iterate over segment, save all values and save to values the chunk_offset.
    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      segment_size = std::distance(segment_it, segment_end);

      // We potentially over-allocate here for better insert performance. We assume that half of the segment's values
      // will be distinct.
      dense_values.reserve(segment_size / 2);
      string_to_chunk_offsets.reserve(segment_size / 2);
      null_values.resize(segment_size);

      for (auto current_position = ChunkOffset{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto& segment_value = segment_item.value();
          // Only insert unique values to avoid a call to std::unique later.
          if (!string_to_chunk_offsets.contains(segment_value)) {
            dense_values.emplace_back(segment_value);
          }
          string_to_chunk_offsets[segment_value].emplace_back(current_position);
        } else {
          null_values[current_position] = true;
        }
      }
    });

    boost::sort::pdqsort(dense_values.begin(), dense_values.end());

    // Compute total compressed data size.
    const auto total_size =
        std::accumulate(dense_values.begin(), dense_values.end(), size_t{0}, [](size_t acc, pmr_string& value) {
          return acc + value.size() + 1;  // + 1 for null byte.
        });

    // Check for oversize dictionary.
    Assert(total_size < std::numeric_limits<uint32_t>::max(), "Dictionary is too large.");

    // Character array containing all distinct strings. This array will later serve as the dictionary.
    auto clob = pmr_vector<char>(total_size);

    // Maps string to offset in clob. We assume that the largest offfet for all strings will still fit into a uint32_t,
    // thus limiting the segment size to 4 GByte.
    auto string_offsets = boost::unordered_flat_map<pmr_string, uint32_t>{};
    // Maps string to ValueID for attribute vector.
    auto string_value_ids = boost::unordered_flat_map<pmr_string, ValueID>{};

    const auto dense_value_count = dense_values.size();
    string_offsets.reserve(dense_value_count);
    string_value_ids.reserve(dense_value_count);

    auto last_offset = uint32_t{0};
    auto last_value_id = ValueID{0};

    // Construct clob with null bytes (therefore + 1).
    for (const auto& value : dense_values) {
      const auto value_size_with_null_byte = value.size() + 1;
      std::memcpy(clob.data() + last_offset, value.c_str(), value_size_with_null_byte);
      string_offsets[value] = last_offset;
      string_value_ids[value] = last_value_id;
      last_offset += value_size_with_null_byte;
      ++last_value_id;
    }

    // Maps ChunkOffset to ValueID.
    auto chunk_offset_to_value_id = pmr_vector<uint32_t>(segment_size);
    auto offset_vector = pmr_vector<uint32_t>(dense_value_count);

    // Construct attribute and offset vector.
    for (const auto& [string, chunk_offsets] : string_to_chunk_offsets) {
      const auto offset = string_offsets[string];
      const auto value_id = string_value_ids[string];
      offset_vector[value_id] = offset;
      for (const auto chunk_offset : chunk_offsets) {
        chunk_offset_to_value_id[chunk_offset] = value_id;
      }
    }

    // Fix up NULL values (mapping only maps actual values, no NULL values).
    const auto null_value_id = dense_value_count;
    for (auto offset = ChunkOffset{0}; offset < segment_size; ++offset) {
      const auto is_null = null_values[offset];
      if (is_null) {
        chunk_offset_to_value_id[offset] = null_value_id;
      }
    }

    // `last_value_id` corresponds to the largest value id of the segment.
    auto compressed_chunk_offset_to_value_id = compress_vector(
        chunk_offset_to_value_id, SegmentEncoder<VariableStringDictionaryEncoder>::vector_compression_type(), allocator,
        {last_value_id});
    return std::make_shared<VariableStringDictionarySegment<pmr_string>>(
        std::move(clob), std::move(compressed_chunk_offset_to_value_id), std::move(offset_vector));
  }
};

}  // namespace hyrise
