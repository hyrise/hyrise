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

  // TODO (anyone): check if this method is needed for FSST
  template <typename T>
  std::shared_ptr<AbstractEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                     const PolymorphicAllocator<T>& allocator) {
    auto values = pmr_vector<T>{};
    auto null_values = pmr_vector<bool>{allocator};
    bool has_null_values = false;

    segment_iterable.with_iterators([&](auto it, auto end) {
      // Early out for empty segments, code below assumes it to be non-empty
      // is this needed?
      if (it == end) {  // TODO: remove
        return;
      }

      const auto segment_size = static_cast<size_t>(std::distance(it, end));
      values.reserve(segment_size);
      null_values.reserve(segment_size);

      // Init is_current_null such that it does not equal the first entry
      T current_value;       // = T{};
      bool is_current_null;  //= !it->is_null();
                             //      SegmentPosition<T> segment_value;
      //      auto current_index = 0u;

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

    // The resize method of the vector might have overallocated memory - hand that memory back to the system
    values.shrink_to_fit();
    null_values.shrink_to_fit();

    pmr_vector<uint32_t> offsets{allocator};
    pmr_vector<unsigned char> compressed_values{allocator};
    std::optional<pmr_vector<bool>> null_values_optional = std::nullopt;
    fsst_decoder_t decoder;

    uint8_t reference_offsets_size = 8;
    pmr_vector<uint64_t> reference_offsets{reference_offsets_size, allocator};

    if (values.size() == 0) {
      offsets.resize(1);
      auto compressed_offsets = compress_vector(offsets, vector_compression_type(), allocator, {offsets.back()});
      return std::make_shared<FSSTSegment<T>>(compressed_values, compressed_offsets, reference_offsets,
                                              null_values_optional, 0, decoder);
    }

    // our temporary data structure keeping char pointer and their length
    std::vector<unsigned long> row_lengths;
    std::vector<unsigned char*> row_pointers;
    row_lengths.reserve(values.size());
    row_pointers.reserve(values.size());

    // needed for compression
    pmr_vector<uint64_t> compressed_value_lengths;
    pmr_vector<unsigned char*> compressed_value_pointers;
    compressed_value_lengths.resize(values.size());
    compressed_value_pointers.resize(values.size());

    offsets.resize(values.size() + 1);

    uint64_t total_length = 0LL;

    for (pmr_string& value : values) {
      total_length += value.size();
      row_lengths.push_back(value.size());
      row_pointers.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));  // TODO: value.c_str()
    }
    compressed_values.resize(16 + 2 * total_length);  // why 16? need to find out

    fsst_encoder_t* encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);

    //  size_t number_compressed_strings = TODO(anyone): avoid error about unused variable in release mode
    fsst_compress(encoder, values.size(), row_lengths.data(), row_pointers.data(), compressed_values.size(),
                  compressed_values.data(), compressed_value_lengths.data(), compressed_value_pointers.data());

    //  DebugAssert(number_compressed_strings == values.size(), "Compressed values buffer size was not big enough");

    decoder = fsst_decoder(encoder);
    fsst_destroy(encoder);

    uint64_t n_elements_in_reference_bucket = static_cast<uint64_t>(
        std::ceil(static_cast<double>(offsets.size()) / static_cast<double>(reference_offsets_size + 1)));
    size_t compressed_values_size = compressed_value_lengths.size();

    // Calculate global offset based on compressed value lengths
    offsets[0] = 0;
    uint64_t aggregated_offset_sum = 0;
    for (size_t index{1}; index <= compressed_values_size; ++index) {
      aggregated_offset_sum += compressed_value_lengths[index - 1];
      offsets[index] = aggregated_offset_sum;
    }
    compressed_values.resize(aggregated_offset_sum);

    // Calculate start offset of each reference bucket
    for (size_t index = 0; index < reference_offsets_size; ++index) {
      reference_offsets[index] = offsets[(index + 1) * n_elements_in_reference_bucket];
    }

    for (size_t index{n_elements_in_reference_bucket}; index <= compressed_values_size; ++index) {
      auto reference_offset_index = (index / n_elements_in_reference_bucket) - 1;

      auto reference_offset = reference_offsets[reference_offset_index];
      offsets[index] = offsets[index] - reference_offset;
    }

    if (has_null_values) {
      null_values_optional = std::make_optional(null_values);
    }

    uint32_t max_offset = 0;
    // TODO: only look at last value in bucket
    for (auto offset : offsets) {
      max_offset = std::max(max_offset, offset);
    }

    auto compressed_offsets = compress_vector(offsets, VectorCompressionType::BitPacking, allocator, {max_offset});

    auto result = std::make_shared<FSSTSegment<T>>(compressed_values, compressed_offsets, reference_offsets,
                                                   null_values_optional, n_elements_in_reference_bucket, decoder);
    return result;
  }
};

}  // namespace opossum