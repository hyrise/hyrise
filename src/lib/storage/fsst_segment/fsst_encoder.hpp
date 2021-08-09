#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <string>

#include "storage/base_segment_encoder.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

class FSSTEncoder : public SegmentEncoder<FSSTEncoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::FSST>;
  static constexpr auto _uses_vector_compression = true;

  template <typename T>
  bool _collect_values(const AnySegmentIterable<T>& segment_iterable, pmr_vector<T>& values,
                       pmr_vector<bool>& null_values) {
    bool has_null_values = false;

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty
      if (it == end) {
        return;
      }

      const auto segment_size = static_cast<size_t>(std::distance(it, end));
      values.reserve(segment_size);
      null_values.reserve(segment_size);

      T current_value;
      bool is_current_null;

      for (; it != end; ++it) {
        auto segment_value = *it;

        current_value = segment_value.value();
        is_current_null = segment_value.is_null();
        if (is_current_null) {
          values.emplace_back("");
          has_null_values = true;
        } else {
          values.emplace_back(current_value);
        }
        null_values.emplace_back(is_current_null);
      }
    });

    // The resize method of the vector might have overallocated memory - hand that memory back to the system.
    values.shrink_to_fit();
    null_values.shrink_to_fit();

    return has_null_values;
  }

  template <typename T>
  fsst_decoder_t _compress_values(pmr_vector<T>& values, pmr_vector<unsigned char>& compressed_values,
                                  pmr_vector<uint64_t>& compressed_value_lengths) {
    // Temporary data structures keeping char pointers and their lengths.
    std::vector<uint64_t> row_lengths;
    std::vector<unsigned char*> row_pointers;
    row_lengths.reserve(values.size());
    row_pointers.reserve(values.size());

    // Needed for compression.
    pmr_vector<unsigned char*> compressed_value_pointers;
    compressed_value_pointers.resize(values.size());
    compressed_value_lengths.resize(values.size());

    uint64_t total_length = 0LL;

    for (pmr_string& value : values) {
      total_length += value.size();
      row_lengths.push_back(value.size());
      // TODO(anybody): refactor the cast
      row_pointers.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));
    }

    // 7 + 2 * total_length -> See fsst_compress interface in fsst.h description.
    compressed_values.resize(7 + 2 * total_length);

    fsst_encoder_t* encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);
    [[maybe_unused]] size_t number_compressed_strings =
        fsst_compress(encoder, values.size(), row_lengths.data(), row_pointers.data(), compressed_values.size(),
                      compressed_values.data(), compressed_value_lengths.data(), compressed_value_pointers.data());

    DebugAssert(number_compressed_strings == values.size(), "Compressed values buffer size was not large enough");

    fsst_decoder_t decoder = fsst_decoder(encoder);
    fsst_destroy(encoder);

    return decoder;
  }

  uint64_t _create_offsets(pmr_vector<uint64_t>& compressed_value_lengths, pmr_vector<uint32_t>& offsets) {
    size_t compressed_values_size = compressed_value_lengths.size();

    // Calculate global offset based on compressed value lengths.
    offsets[0] = 0;
    uint64_t aggregated_offset_sum = 0;
    for (size_t index{1}; index <= compressed_values_size; ++index) {
      aggregated_offset_sum += compressed_value_lengths[index - 1];
      offsets[index] = aggregated_offset_sum;
    }

    return aggregated_offset_sum;
  }

  uint64_t _create_reference_offsets(pmr_vector<uint32_t>& offsets, pmr_vector<uint64_t>& reference_offsets) {
    auto reference_offsets_size = reference_offsets.size();
    auto offsets_size = offsets.size();
    // Since the first reference offset (which is always 0) is not saved,
    // there are reference_offsets_size + 1 reference buckets.
    if (offsets_size < reference_offsets_size + 1) {
      // Do not use reference offsets.
      return 0;
    }

    uint64_t n_elements_in_reference_bucket = offsets_size / (reference_offsets_size + 1);

    // Calculate start offset of each reference bucket.
    for (size_t index = 0; index < reference_offsets_size; ++index) {
      reference_offsets[index] = offsets[(index + 1) * n_elements_in_reference_bucket];
    }

    // Substract the reference offset from the original offset (create "zig-zag" pattern).
    for (size_t index{n_elements_in_reference_bucket}; index < offsets_size; ++index) {
      auto reference_offset_index = (index / n_elements_in_reference_bucket) - 1;

      // Merge the last uncompleted bucket with the second last bucket.
      reference_offset_index = std::min(reference_offset_index, reference_offsets_size - 1);

      auto reference_offset = reference_offsets[reference_offset_index];
      offsets[index] = offsets[index] - reference_offset;
    }

    return n_elements_in_reference_bucket;
  }

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    auto values = pmr_vector<T>{};
    auto null_values = pmr_vector<bool>{allocator};
    std::optional<pmr_vector<bool>> null_values_optional = std::nullopt;

    // Fill values and null_values vector accordingly.
    bool has_null_values = _collect_values(segment_iterable, values, null_values);

    if (has_null_values) {
      null_values_optional = std::make_optional(null_values);
    }

    pmr_vector<unsigned char> compressed_values{allocator};
    pmr_vector<uint64_t> compressed_value_lengths;
    fsst_decoder_t decoder;

    if (values.size() != 0) {
      // Compress the values using FSST third party library.
      decoder = _compress_values(values, compressed_values, compressed_value_lengths);
    }

    // Create an offsets container (aggregated_offset_sum is the total size of the compressed values).
    pmr_vector<uint32_t> offsets{allocator};
    offsets.resize(values.size() + 1);
    uint64_t aggregated_offset_sum = _create_offsets(compressed_value_lengths, offsets);

    // "shrink_to_fit" to the total size of the compressed strings.
    compressed_values.resize(aggregated_offset_sum);

    // Create reference offsets as well as create the "zig-zag" pattern in offsets
    // in order to achieve larger vector compression rates.
    uint8_t reference_offsets_size = 8;
    pmr_vector<uint64_t> reference_offsets{reference_offsets_size, 0, allocator};
    uint64_t n_elements_in_reference_bucket = _create_reference_offsets(offsets, reference_offsets);

    // Find maximum value in offsets in order to use it in later vector compression.
    uint32_t max_offset = *std::max_element(offsets.begin(), offsets.end());

    // Hardcode BitPacking as vector compression type.
    auto compressed_offsets = compress_vector(offsets, VectorCompressionType::BitPacking, allocator, {max_offset});

    return std::make_shared<FSSTSegment<T>>(compressed_values, compressed_offsets, reference_offsets,
                                            null_values_optional, n_elements_in_reference_bucket, decoder);
  }
};

}  // namespace opossum
