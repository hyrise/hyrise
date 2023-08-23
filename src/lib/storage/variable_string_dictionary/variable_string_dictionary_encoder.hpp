#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>

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
    auto dense_values = std::vector<pmr_string>();  // Contains the actual values (no NULLs).
    auto null_values = std::vector<bool>();         // bitmap to mark NULL values
    // Maps string to ChunkOffsets for faster write of vector that maps ChunkOffsets to ValueID.
    auto string_to_chunk_offsets = std::unordered_map<pmr_string, std::vector<ChunkOffset>>();
    auto segment_size = uint32_t{0};

    // Iterate over segment, save all values and save to values the chunk_offset.
    segment_iterable.with_iterators([&](auto segment_it, const auto segment_end) {
      segment_size = std::distance(segment_it, segment_end);
      dense_values.reserve(segment_size);  // Potentially overallocate for segments with NULLs.
      null_values.resize(segment_size);    // Resized to size of segment.

      for (auto current_position = size_t{0}; segment_it != segment_end; ++segment_it, ++current_position) {
        const auto segment_item = *segment_it;
        if (!segment_item.is_null()) {
          const auto& segment_value = segment_item.value();
          // Only insert unique values to avoid a call to std::unique later.
          if (!string_to_chunk_offsets.contains(segment_value)) {
            dense_values.push_back(segment_value);
          }
          string_to_chunk_offsets[segment_value].push_back(ChunkOffset(current_position));
        } else {
          null_values[current_position] = true;
        }
      }
    });

    // Eliminate duplicate strings.
    std::sort(dense_values.begin(), dense_values.end());
    dense_values.shrink_to_fit();

    // Compute total compressed data size.
    const auto total_size = std::accumulate(dense_values.begin(), dense_values.end(), size_t{0},
                                            [](size_t acc, pmr_string& value) { return acc + value.size(); });

    // Check for oversize dictionary.
    Assert(total_size < std::numeric_limits<uint32_t>::max(), "Dictionary is too large!");

    // uniform character array containing all distinct strings
    auto clob = std::make_shared<pmr_vector<char>>(pmr_vector<char>(total_size));
    // We assume segment size up to 4 GByte.
    // Maps string to offset in clob.
    auto string_offsets = std::unordered_map<pmr_string, uint32_t>();
    // Maps string to ValueID for attribute vector.
    auto string_value_ids = std::unordered_map<pmr_string, ValueID>();
    auto last_offset = uint32_t{0};
    auto last_value_id = ValueID{0};

    // Construct clob without null bytes.
    for (const auto& value : dense_values) {
      memcpy(clob->data() + last_offset, value.c_str(), value.size());
      string_offsets[value] = static_cast<uint32_t>(last_offset);
      string_value_ids[value] = last_value_id++;
      last_offset += value.size();
    }

    // Maps ChunkOffset to ValueID.
    auto chunk_offset_to_value_id = pmr_vector<uint32_t>(static_cast<size_t>(segment_size));
    auto offset_vector = std::make_shared<pmr_vector<uint32_t>>(pmr_vector<uint32_t>(dense_values.size()));
    offset_vector->shrink_to_fit();

    // Construct attribute and offset vector.
    for (const auto& [string, chunk_offsets] : string_to_chunk_offsets) {
      const auto offset = string_offsets[string];
      const auto value_id = string_value_ids[string];
      (*offset_vector)[value_id] = offset;
      for (const auto chunk_offset : chunk_offsets) {
        chunk_offset_to_value_id[chunk_offset] = value_id;
      }
    }

    // Fix up null values (mapping only maps actual data, no null values).
    const auto null_value_id = dense_values.size();
    for (auto offset = ChunkOffset{0}; offset < segment_size; ++offset) {
      const auto is_null = null_values[offset];
      if (is_null) {
        chunk_offset_to_value_id[offset] = null_value_id;
      }
    }

    // last_value_id corresponds to the maximal value id of the segment.
    const auto compressed_chunk_offset_to_value_id = std::shared_ptr<const BaseCompressedVector>(compress_vector(
        chunk_offset_to_value_id, SegmentEncoder<VariableStringDictionaryEncoder>::vector_compression_type(), allocator,
        {last_value_id}));

    return std::make_shared<VariableStringDictionarySegment<pmr_string>>(clob, compressed_chunk_offset_to_value_id,
                                                                         offset_vector);
  }
};

}  // namespace hyrise
