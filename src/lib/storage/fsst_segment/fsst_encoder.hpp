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
  std::tuple<pmr_vector<T>, std::optional<pmr_vector<bool>>> _collect_values(
      const AnySegmentIterable<T>& segment_iterable, const PolymorphicAllocator<T>& allocator) {
    auto values = pmr_vector<T>{};
    auto null_values = pmr_vector<bool>{allocator};
    std::optional<pmr_vector<bool>> null_values_optional;
    bool has_null_values = false;

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty.
      if (it == end) {
        return;
      }

      const auto segment_size = std::distance(it, end);
      values.reserve(segment_size);
      null_values.reserve(segment_size);

      for (; it != end; ++it) {
        auto segment_value = *it;

        T current_value = segment_value.value();
        auto is_current_null = segment_value.is_null();
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

    if (has_null_values) {
      null_values_optional = std::make_optional(null_values);
    }

    return std::make_tuple(std::move(values), std::move(null_values_optional));
  }

  template <typename T>
  std::tuple<pmr_vector<unsigned char>, pmr_vector<uint64_t>, fsst_decoder_t> _compress_values(
      pmr_vector<T>& values, const PolymorphicAllocator<T>& allocator) {
    // Return variables.
    auto compressed_values = pmr_vector<unsigned char>{allocator};
    auto compressed_value_lengths = pmr_vector<uint64_t>{};
    fsst_decoder_t decoder;

    if (values.size() != 0) {
      // Temporary data structures keeping char pointers and their lengths.
      std::vector<uint64_t> row_lengths;
      std::vector<unsigned char*> row_pointers;
      row_lengths.reserve(values.size());
      row_pointers.reserve(values.size());

      // Needed for compression.
      pmr_vector<unsigned char*> compressed_value_pointers;
      compressed_value_pointers.resize(values.size());
      compressed_value_lengths.resize(values.size());

      auto total_length = uint64_t{0};

      for (auto& value : values) {
        total_length += value.size();
        row_lengths.push_back(value.size());
        row_pointers.push_back(reinterpret_cast<unsigned char*>(value.data()));
      }

      // 7 + 2 * total_length -> See fsst_compress interface in fsst.h description.
      compressed_values.resize(7 + 2 * total_length);

      fsst_encoder_t* encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);
      [[maybe_unused]] size_t number_compressed_strings =
          fsst_compress(encoder, values.size(), row_lengths.data(), row_pointers.data(), compressed_values.size(),
                        compressed_values.data(), compressed_value_lengths.data(), compressed_value_pointers.data());

      DebugAssert(number_compressed_strings == values.size(), "Compressed values buffer size was not large enough");

      decoder = fsst_decoder(encoder);
      fsst_destroy(encoder);
    }

    return std::make_tuple(std::move(compressed_values), std::move(compressed_value_lengths), decoder);
  }

  template <typename T>
  pmr_vector<uint32_t> _create_offsets(const pmr_vector<uint64_t>& compressed_value_lengths,
                                       const PolymorphicAllocator<T>& allocator) {
    const auto compressed_values_size = compressed_value_lengths.size();
    auto offsets = pmr_vector<uint32_t>{allocator};
    offsets.resize(compressed_values_size + 1);

    // Calculate global offset based on compressed value lengths.
    offsets[0] = 0;
    auto aggregated_offset_sum = uint64_t{0};
    for (auto index = size_t{1}; index <= compressed_values_size; ++index) {
      aggregated_offset_sum += compressed_value_lengths[index - 1];
      offsets[index] = aggregated_offset_sum;
    }

    return offsets;
  }

  template <typename T>
  std::tuple<pmr_vector<uint64_t>, uint32_t> _create_reference_offsets(pmr_vector<uint32_t>& offsets,
                                                                       const PolymorphicAllocator<T>& allocator) {
    auto reference_offsets_size = size_t{8};
    pmr_vector<uint64_t> reference_offsets{reference_offsets_size, 0, allocator};
    auto offsets_size = offsets.size();

    // Since the first reference offset (which is always 0) is not saved,
    // there are reference_offsets_size + 1 reference buckets.
    if (offsets_size < reference_offsets_size + 1) {
      // Do not use reference offsets.
      return std::make_tuple(reference_offsets, 0);
    }

    const auto n_elements_in_reference_bucket = static_cast<uint32_t>(offsets_size / (reference_offsets_size + 1));

    // Calculate start offset of each reference bucket.
    for (auto index = size_t{0}; index < reference_offsets_size; ++index) {
      reference_offsets[index] = offsets[(index + 1) * n_elements_in_reference_bucket];
    }

    // Subtract the reference offset from the original offsets.
    for (auto index = size_t{n_elements_in_reference_bucket}; index < offsets_size; ++index) {
      auto reference_offset_index = (index / n_elements_in_reference_bucket) - 1;

      // Move last unmatched elements to the last bucket.
      //
      // Example:
      // offset_size = 200, number of elements per bucket = 200 / (8 + 1) = 22. However,
      // 22 * 9 = 198 != 200. So, the last 2 Elements refer to the 10th bucket, but we
      // only have 9 (and we save 8 reference offsets, since the reference offset in the
      // 1st bucket is 0). So, the last 2 Elements will be added to the last (9th) bucket.
      reference_offset_index = std::min(reference_offset_index, reference_offsets_size - 1);

      auto reference_offset = reference_offsets[reference_offset_index];
      offsets[index] = offsets[index] - reference_offset;
    }

    return std::make_tuple(std::move(reference_offsets), n_elements_in_reference_bucket);
  }

  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    // Collect segment values.
    auto [values, null_values] = _collect_values(segment_iterable, allocator);

    // Compress the values using FSST third party library.
    auto [compressed_values, compressed_value_lengths, decoder] = _compress_values(values, allocator);

    // Create offsets.
    auto offsets = _create_offsets(compressed_value_lengths, allocator);

    // "shrink_to_fit" to the total size of the compressed strings.
    auto aggregated_offset_sum = offsets.back();
    compressed_values.resize(aggregated_offset_sum);

    // Create reference offsets and substract them from offsets
    // in order to achieve larger vector compression rates.
    auto [reference_offsets, n_elements_in_reference_bucket] = _create_reference_offsets(offsets, allocator);

    // Find maximum value in offsets in order to use it in later vector compression.
    const auto max_offset = *std::max_element(offsets.cbegin(), offsets.cend());

    // Hardcode BitPacking as vector compression type.
    auto compressed_offsets = compress_vector(offsets, VectorCompressionType::BitPacking, allocator, {max_offset});

    return std::make_shared<FSSTSegment<T>>(compressed_values, compressed_offsets, reference_offsets, null_values,
                                            n_elements_in_reference_bucket, decoder);
  }
};

}  // namespace opossum
