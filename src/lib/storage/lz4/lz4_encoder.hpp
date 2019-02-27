#pragma once

#include <lz4hc.h>
#include <lib/dictBuilder/zdict.h>

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

//  template <typename T>
//  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<T>>& value_segment) {
//    const auto alloc = value_segment->values().get_allocator();
//    const auto num_elements = value_segment->size();
//    DebugAssert(num_elements <= std::numeric_limits<int>::max(),
//                "Trying to compress a ValueSegment with more "
//                "elements than fit into an int.");
//
//    // TODO(anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
//    // copying it element by element
//    auto values = pmr_vector<T>{alloc};
//    values.reserve(num_elements);
//    auto null_values = pmr_vector<bool>{alloc};
//    null_values.reserve(num_elements);
//
//    // copy values and null flags from value segment
//    auto iterable = ValueSegmentIterable<T>{*value_segment};
//    iterable.with_iterators([&](auto it, auto end) {
//      for (; it != end; ++it) {
//        auto segment_value = *it;
//        values.emplace_back(segment_value.value());
//        null_values.emplace_back(segment_value.is_null());
//      }
//    });
//
//    // lz4 stream compression in blocks
//    const auto input_size = static_cast<int>(values.size() * sizeof(T));
//    const int block_size = 4096;
//    const int compression_level = LZ4HC_CLEVEL_MAX;
//
//    std::shared_ptr<pmr_vector<char>> dictionary_ptr;
//    if (input_size > block_size) {
//      // train a dictionary in advance for better compression
//      dictionary_ptr = _generate_dictionary(values);
//    } else {
//      dictionary_ptr = nullptr;
//    }
//
//    LZ4_streamHC_t* stream_ptr = LZ4_createStreamHC();
//    LZ4_resetStreamHC(stream_ptr, compression_level);
//
//    auto compressed_data = pmr_vector<char>{alloc};
//    auto num_blocks = input_size / block_size + 1;
//    size_t compressed_data_bound = LZ4_compressBound(block_size * num_blocks);
//    compressed_data.reserve(compressed_data_bound);
//
//    size_t compressed_data_size = 0;
//    auto offsets = pmr_vector<size_t>{alloc};
//    offsets.resize(num_blocks);
//
//    for (int block_count = 0; block_count < num_blocks; ++block_count) {
//
//      const auto decompressed_block_size = block_count + 1 == num_blocks ? input_size - (block_count * block_size) : block_size;
//      auto compressed_block_bound = LZ4_compressBound(decompressed_block_size);
//      auto compressed_block = pmr_vector<char>{alloc};
//      compressed_block.resize(static_cast<size_t>(compressed_block_bound));
//
//      int outlen;
//      auto block_ptr = reinterpret_cast<char*>(values.data()) + (block_count * block_size);
//      int block_length = block_count + 1 == num_blocks ? input_size - (block_size * block_count) : block_size;
//
//      if (dictionary_ptr != nullptr) {
//        // Forget previously compressed data and load the dictionary, so blocks are compressed independently
//        LZ4_loadDictHC(stream_ptr, dictionary_ptr->data(), static_cast<int>(dictionary_ptr->size()));
//      }
//      outlen = LZ4_compress_HC_continue(stream_ptr, block_ptr, compressed_block.data(),
//                                        block_length, compressed_block_bound);
//      if (outlen <= 0) {
//        Fail("LZ4 stream compression failed");
//      }
//      compressed_data_size += outlen;
//      offsets.at(block_count) = compressed_data_size;
//      compressed_block.resize(outlen);
//      compressed_data.insert(compressed_data.end(), compressed_block.begin(), compressed_block.end());
//    }
//
//    LZ4_freeStreamHC(stream_ptr);
//    if (compressed_data_bound < compressed_data_size) {
//      Fail("LZ4 stream compression exceeded bounds");
//    }
//
//    compressed_data.resize(compressed_data_size);
//
//    auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
//    auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
//    auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));
//
//    return std::allocate_shared<LZ4Segment<T>>(alloc, data_ptr, null_values_ptr, offset_ptr, compressed_data_size,
//                                               input_size, num_elements, dictionary_ptr);
//  }

//    std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<std::string>>& value_segment) {
//      const auto alloc = value_segment->values().get_allocator();
//      const auto num_elements = value_segment->size();
//      DebugAssert(num_elements <= std::numeric_limits<int>::max(),
//                  "Trying to compress a ValueSegment with more "
//                  "elements than fit into an int.");
//
//      // TODO(anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
//      // copying it element by element
//      auto values = pmr_vector<char>{alloc};
//      auto null_values = pmr_vector<bool>{alloc};
//      null_values.reserve(num_elements);
//      // the offset is the beginning of the string in the decompressed data vector
//      // to look up the end the offset of the next element has to be looked up (in case of the last element the end is
//      // the end of the vector)
//      // null values are not saved and the offset is the same as the next element (i.e. the previous offset + the length
//      // of the previous value)
//      auto string_offsets = pmr_vector<size_t>{alloc};
//      string_offsets.reserve(num_elements);
//
//      // copy values and null flags from value segment
//      auto iterable = ValueSegmentIterable<std::string>{*value_segment};
//      iterable.with_iterators([&](auto it, auto end) {
//        size_t offset = 0u;
//        bool is_null;
//        for (; it != end; ++it) {
//          auto segment_value = *it;
//          is_null = segment_value.is_null();
//          null_values.push_back(is_null);
//          string_offsets.emplace_back(offset);
//          if (!is_null) {
//            auto c_string = segment_value.value().c_str();
//            auto length = strlen(c_string);
//            values.insert(values.cend(), c_string, c_string + length);
//            offset += length;
//          }
//        }
//      });
//
//      // lz4 stream compression in blocks
//      const int input_size = static_cast<int>(values.size());
//      const int block_size = 4096;
//      const int compression_level = LZ4HC_CLEVEL_MAX;
//
//      std::shared_ptr<pmr_vector<char>> dictionary_ptr;
//      if (input_size > block_size) {
//        // train a dictionary in advance for better compression
//        dictionary_ptr = _generate_dictionary(values);
//      } else {
//        dictionary_ptr = nullptr;
//      }
//
//      LZ4_streamHC_t* stream_ptr = LZ4_createStreamHC();
//      LZ4_resetStreamHC(stream_ptr, compression_level);
//
//      auto compressed_data = pmr_vector<char>{alloc};
//      auto num_blocks = input_size / block_size + 1;
//      size_t compressed_data_bound = LZ4_compressBound(block_size * num_blocks);
//      compressed_data.reserve(compressed_data_bound);
//
//      size_t compressed_data_size = 0;
//      auto offsets = pmr_vector<size_t>{alloc};
//      offsets.resize(num_blocks);
//
//      for (int block_count = 0; block_count < num_blocks; ++block_count) {
//
//        const auto decompressed_block_size = block_count + 1 == num_blocks ? input_size - (block_count * block_size) : block_size;
//        auto compressed_block_bound = LZ4_compressBound(decompressed_block_size);
//        auto compressed_block = pmr_vector<char>{alloc};
//        compressed_block.resize(static_cast<size_t>(compressed_block_bound));
//
//        int outlen;
//        auto block_ptr = reinterpret_cast<char*>(values.data()) + (block_count * block_size);
//        int block_length = block_count + 1 == num_blocks ? input_size - (block_size * block_count) : block_size;
//
//        if (dictionary_ptr != nullptr) {
//          // Forget previously compressed data and load the dictionary, so blocks are compressed independently
//          LZ4_loadDictHC(stream_ptr, dictionary_ptr->data(), static_cast<int>(dictionary_ptr->size()));
//        }
//        outlen = LZ4_compress_HC_continue(stream_ptr, block_ptr, compressed_block.data(),
//                                          block_length, compressed_block_bound);
//        if (outlen <= 0) {
//          Fail("LZ4 stream compression failed");
//        }
//        compressed_data_size += outlen;
//        offsets.at(block_count) = compressed_data_size;
//        compressed_block.resize(outlen);
//        compressed_data.insert(compressed_data.end(), compressed_block.begin(), compressed_block.end());
//      }
//
//      LZ4_freeStreamHC(stream_ptr);
//      if (compressed_data_bound < compressed_data_size) {
//        Fail("LZ4 stream compression exceeded bounds");
//      }
//
//      compressed_data.resize(compressed_data_size);
//
//      auto data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(compressed_data));
//      auto null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(null_values));
//      auto offset_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(offsets));
//      auto string_offsets_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(string_offsets));
//
//      return std::allocate_shared<LZ4Segment<std::string>>(alloc, data_ptr, null_values_ptr, offset_ptr, compressed_data_size,
//                                                           input_size, num_elements, dictionary_ptr, string_offsets_ptr);
//    }
//
//  private:
//    template<typename T>
//    std::shared_ptr<pmr_vector<char>> _generate_dictionary(const pmr_vector<T> &values) {
//
//      const size_t num_values = values.size();
//      const size_t values_size = num_values * sizeof(T);
//      const size_t sample_size = sizeof(T) > 8 ? sizeof(T) : 8; // 8 bytes is the minimum size of a sample
//      const size_t num_samples = values_size / sample_size;
//      const std::vector<size_t> sample_lens(num_samples, sample_size); // all samples are of the same size
//      size_t max_dict_bytes = num_samples * sample_size / 100; // recommended dict size is about 1/100th of size of all samples
//      if (max_dict_bytes < 1000) {
//        max_dict_bytes = 1000; // but at least 1KB, smaller dictionaries won't work
//      }
//      auto dict_data = pmr_vector<char>{values.get_allocator()};
//      dict_data.resize(max_dict_bytes);
//
//      const size_t dict_len = ZDICT_trainFromBuffer(
//        reinterpret_cast<void*>(dict_data.data()),
//        max_dict_bytes,
//        reinterpret_cast<const void*>(values.data()),
//        sample_lens.data(),
//        static_cast<unsigned>(sample_lens.size()));
//
//      if (ZDICT_isError(dict_len)) {
//        Fail("ZSTD dictionary generation failed");
//      }
//      DebugAssert(dict_len <= max_dict_bytes, "Dictionary is larger than the memory allocated for it.");
//      dict_data.resize(dict_len);
//
//      return std::make_shared<pmr_vector<char>>(dict_data);
//    }

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
