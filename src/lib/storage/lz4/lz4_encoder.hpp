#pragma once

#include <algorithm>
#include <array>
#include <limits>
#include <memory>
#include <string>

#include "storage/base_segment_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/enum_constant.hpp"
#include "utils/assert.hpp"

#include "lib/lz4hc.h"

namespace opossum {

class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();
    DebugAssert(num_elements <= std::numeric_limits<int>::max(), "Trying to compress a ValueSegment with more "
                                                                 "elements than fit into an int.");

    // TODO (anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
    // copying it element by element
    auto values = pmr_vector<T>{alloc};
    values.reserve(num_elements);
    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(num_elements);

    // copy values and null flags from value segment
    auto iterable = ValueSegmentIterable<T>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      for (; it != end; ++it) {
        auto segment_value = *it;
        values.emplace_back(segment_value.value());
        null_values.push_back(segment_value.is_null());
      }
    });

    // lz4 compression
    const auto input_size = static_cast<int>(values.size() * sizeof(T));
    auto output_size = LZ4_compressBound(input_size);
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.reserve(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(reinterpret_cast<char*>(values.data()), compressed_data.data(),
                                                   input_size, output_size, LZ4HC_CLEVEL_MAX);
    if (compression_result <= 0) {
      Fail("LZ4 compression failed");
    }

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));

    return std::allocate_shared<LZ4Segment<T>>(alloc, data_ptr, null_values_ptr, nullptr, compression_result,
                                               input_size, num_elements);
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<std::string>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();

    // copy values and null flags from value segment
    auto values = pmr_vector<char>{alloc};
    auto null_values = pmr_vector<bool>{alloc};
    null_values.reserve(num_elements);
    // the offset is the beginning of the string in the decompressed data vector
    // to look up the end the offset of the next element has to be looked up (in case of the last element the end is
    // the end of the vector)
    // null values are not saved and the offset is the same as the next element (i.e. the previous offset + the length
    // of the previous value)
    auto offsets = pmr_vector<size_t>{alloc};
    offsets.reserve(num_elements);

    auto iterable = ValueSegmentIterable<std::string>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      size_t offset = 0u;
      bool is_null;
      for (; it != end; ++it) {
        auto segment_value = *it;
        is_null = segment_value.is_null();
        null_values.push_back(is_null);
        offsets.emplace_back(offset);
        if (!is_null) {
          auto c_string = segment_value.value().c_str();
          auto length = strlen(c_string);
          values.insert(values.cend(), c_string, c_string + length);
          offset += length;
        }
      }
    });

    // lz4 compression
    const auto input_size = static_cast<int>(values.size());
    auto output_size = LZ4_compressBound(input_size);
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.reserve(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(values.data(), compressed_data.data(),
                                                   input_size, output_size, LZ4HC_CLEVEL_MAX);
    if (compression_result <= 0) {
      Fail("LZ4 compression failed");
    }

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));

    return std::allocate_shared<LZ4Segment<std::string>>(alloc, data_ptr, null_values_ptr, offset_ptr,
                                                         compression_result, input_size, num_elements);
  }
};

}  // namespace opossum
