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
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

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
    DebugAssert(num_elements <= std::numeric_limits<int>::max(),
                "Trying to compress a ValueSegment with more "
                "elements than fit into an int.");

    // TODO(anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
    // copying it element by element
    auto values = pmr_vector<T>{alloc};
    values.resize(num_elements);
    auto null_values = pmr_vector<bool>{alloc};
    null_values.resize(num_elements);

    // copy values and null flags from value segment
    auto iterable = ValueSegmentIterable<T>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      // iterate over the iterator to access the values and increment the row index to write to the values and null
      // values vectors
      for (size_t row_index = 0u; it != end; ++it, ++row_index) {
        auto segment_value = *it;
        values[row_index] = segment_value.value();
        null_values[row_index] = segment_value.is_null();
      }
    });

    /**
     * Use the LZ4 high compression API to compress the copied values. As C-library LZ4 needs raw pointers as input
     * and output. To avoid directly handling raw pointers we use std::vectors as input and output. The input vector
     * contains the data that needs to be compressed and the output vector is allocated enough memory to contain
     * the compression result. Via the .data() call we can supply LZ4 with raw pointers to the memory the vectors use.
     * These are cast to char-pointers since LZ4 expects char pointers.
     */
    const auto input_size = static_cast<int>(values.size() * sizeof(T));
    // estimate the (maximum) output size
    auto output_size = LZ4_compressBound(input_size);
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.resize(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(reinterpret_cast<char*>(values.data()), compressed_data.data(),
                                                   input_size, output_size, LZ4HC_CLEVEL_MAX);
    Assert(compression_result > 0, "LZ4 compression failed");

    // shrink the vector to the actual size of the compressed result
    compressed_data.resize(static_cast<size_t>(compression_result));

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));

    return std::allocate_shared<LZ4Segment<T>>(alloc, data_ptr, null_values_ptr, nullptr, compression_result,
                                               input_size, num_elements);
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(
      const std::shared_ptr<const ValueSegment<std::string>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();
    DebugAssert(num_elements <= std::numeric_limits<int>::max(),
                "Trying to compress a ValueSegment with more "
                "elements than fit into an int.");

    // copy values and null flags from value segment
    auto values = pmr_vector<char>{alloc};
    auto null_values = pmr_vector<bool>{alloc};
    null_values.resize(num_elements);

    /**
     * These offsets mark the beginning of strings (and therefore end of the previous string) in the data vector.
     * These offsets are character offsets. The string at position 0 starts at the offset stored at position 0.
     * Its exclusive end is the offset stored at position 1 (i.e. offsets[1] - 1 is the last character of the string
     * at position 0).
     * In case of the last string its end is determined by the end of the data vector.
     */
    auto offsets = pmr_vector<size_t>{alloc};
    offsets.resize(num_elements);

    auto iterable = ValueSegmentIterable<std::string>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      size_t offset = 0u;
      bool is_null;
      // iterate over the iterator to access the values and increment the row index to write to the values and null
      // values vectors
      for (size_t row_index = 0; it != end; ++it, ++row_index) {
        auto segment_value = *it;
        is_null = segment_value.is_null();
        null_values[row_index] = is_null;
        offsets[row_index] = offset;
        if (!is_null) {
          auto data = segment_value.value();
          values.insert(values.cend(), data.begin(), data.end());
          offset += data.size();
        }
      }
    });

    /**
     * Use the LZ4 high compression API to compress the copied values. As C-library LZ4 needs raw pointers as input
     * and output. To avoid directly handling raw pointers we use std::vectors as input and output. The input vector
     * contains the data that needs to be compressed and the output vector is allocated enough memory to contain
     * the compression result. Via the .data() call we can supply LZ4 with raw pointers to the memory the vectors use.
     * These are cast to char-pointers since LZ4 expects char pointers.
     */
    const auto input_size = static_cast<int>(values.size());
    // estimate the (maximum) output size
    auto output_size = LZ4_compressBound(input_size);
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.resize(static_cast<size_t>(output_size));
    const int compression_result =
        LZ4_compress_HC(values.data(), compressed_data.data(), input_size, output_size, LZ4HC_CLEVEL_MAX);
    Assert(compression_result > 0, "LZ4 compression failed");

    // shrink the vector to the actual size of the compressed result
    compressed_data.resize(static_cast<size_t>(compression_result));

    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
    auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));

    return std::allocate_shared<LZ4Segment<std::string>>(alloc, data_ptr, null_values_ptr, offset_ptr,
                                                         compression_result, input_size, num_elements);
  }
};

}  // namespace opossum
