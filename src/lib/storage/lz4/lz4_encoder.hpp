#pragma once

#include <lz4hc.h>

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

namespace opossum {

class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
  static constexpr auto _uses_vector_compression = false;

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();

    // TODO(anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
    // copying it element by element
    auto values = pmr_vector<T>{alloc};
    values.resize(num_elements);
    auto null_values = pmr_vector<bool>{alloc};
    null_values.resize(num_elements);

    // copy values and null flags from value segment
    auto iterable = ValueSegmentIterable<T>{*value_segment};
    iterable.with_iterators([&](auto it, auto end) {
      // iterate over the segment to access the values and increment the row index to write to the values and null
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
    DebugAssert(values.size() * sizeof(T) <= std::numeric_limits<int>::max(),
                "Input of LZ4 encoder contains too many bytes to fit into a 32-bit signed integer sized vector that is"
                " used by the LZ4 library.");

    const auto input_size = values.size() * sizeof(T);
    // estimate the (maximum) output size
    const auto output_size = LZ4_compressBound(static_cast<int>(input_size));
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.resize(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(reinterpret_cast<char*>(values.data()), compressed_data.data(),
                                                   static_cast<int>(input_size), output_size, LZ4HC_CLEVEL_MAX);
    Assert(compression_result > 0, "LZ4 compression failed");

    // shrink the vector to the actual size of the compressed result
    compressed_data.resize(static_cast<size_t>(compression_result));
    compressed_data.shrink_to_fit();

    return std::allocate_shared<LZ4Segment<T>>(alloc, std::move(compressed_data), std::move(null_values), input_size);
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<pmr_string>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();

    /**
     * First iterate over the values for two reasons.
     * 1) If all the strings are empty LZ4 will try to compress an empty vector which will cause a segmentation fault.
     * In this case we can and need to do an early exit.
     * 2) Sum the length of the strings to improve the performance when copying the data to the char vector.
     */
    size_t num_chars = 0u;
    ValueSegmentIterable<pmr_string>{*value_segment}.with_iterators([&](auto it, auto end) {
      for (size_t row_index = 0; it != end; ++it, ++row_index) {
        if (!it->is_null()) {
          num_chars += it->value().size();
        }
      }
    });

    // copy values and null flags from value segment
    auto values = pmr_vector<char>{alloc};
    values.reserve(num_chars);
    auto null_values = pmr_vector<bool>{alloc};
    null_values.resize(num_elements);

    /**
     * These offsets mark the beginning of strings (and therefore end of the previous string) in the data vector.
     * These offsets are character offsets. The string at position 0 starts at the offset stored at position 0, which
     * will always be 0.
     * Its exclusive end is the offset stored at position 1 (i.e. offsets[1] - 1 is the last character of the string
     * at position 0).
     * In case of the last string its end is determined by the end of the data vector.
     */
    auto offsets = pmr_vector<size_t>{alloc};
    offsets.resize(num_elements);

    auto iterable = ValueSegmentIterable<pmr_string>{*value_segment};
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
     * If the input only contained null values and/or empty strings we don't need to compress anything (and LZ4 will
     * cause an error). Therefore we can return the encoded segment already.
     */
    if (!num_chars) {
      return std::allocate_shared<LZ4Segment<pmr_string>>(alloc, pmr_vector<char>{alloc}, std::move(null_values),
                                                          std::move(offsets), 0u);
    }

    /**
     * Use the LZ4 high compression API to compress the copied values. As C-library LZ4 needs raw pointers as input
     * and output. To avoid directly handling raw pointers we use std::vectors as input and output. The input vector
     * contains the data that needs to be compressed and the output vector is allocated enough memory to contain
     * the compression result. Via the .data() call we can supply LZ4 with raw pointers to the memory the vectors use.
     * These are cast to char-pointers since LZ4 expects char pointers.
     */
    DebugAssert(values.size() <= std::numeric_limits<int>::max(),
                "String input of LZ4 encoder contains too many characters to fit into a 32-bit signed integer sized "
                "vector that is used by the LZ4 library.");
    const auto input_size = values.size();
    // estimate the (maximum) output size
    const auto output_size = LZ4_compressBound(static_cast<int>(input_size));
    auto compressed_data = pmr_vector<char>{alloc};
    compressed_data.resize(static_cast<size_t>(output_size));
    const int compression_result = LZ4_compress_HC(values.data(), compressed_data.data(), static_cast<int>(input_size),
                                                   output_size, LZ4HC_CLEVEL_MAX);
    Assert(compression_result > 0, "LZ4 compression failed");

    // shrink the vector to the actual size of the compressed result
    compressed_data.resize(static_cast<size_t>(compression_result));
    compressed_data.shrink_to_fit();

    return std::allocate_shared<LZ4Segment<pmr_string>>(alloc, std::move(compressed_data), std::move(null_values),
                                                        std::move(offsets), input_size);
  }
};

}  // namespace opossum
