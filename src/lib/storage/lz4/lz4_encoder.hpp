#pragma once

#include <lib/dictBuilder/zdict.h>
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
  static constexpr size_t _block_size = 4096u;
  static_assert(_block_size <= std::numeric_limits<int>::max(), "LZ4 block size can't be larger than the maximum size"
                                                                "of a 32 bit signed int");

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
     * Use the LZ4 high compression stream API to compress the copied values. The data is separated into different
     * blocks that are compressed independently. To maintain a high compression ratio and indepdence of these blocks
     * we first use zstd to generate a dictionary with the whole column as input. LZ4 can use the dictionary "learned"
     * on the column to compress the data in blocks.
     *
     * The C-library LZ4 needs raw pointers as input and output. To avoid directly handling raw pointers we use
     * std::vectors as input and output. The input vector contains the block that needs to be compressed and the
     * output vector is allocated enough memory to contain the compression result. Via the .data() call we can supply
     * LZ4 with raw pointers to the memory the vectors use. These are cast to char-pointers since LZ4 expects char
     * pointers.
     */

    const auto input_size = values.size() * sizeof(T);

    // Only if the segment is compressed into more than block it makes sense to use a dictionary.
    auto dictionary = pmr_vector<char>{};
    if (input_size > _block_size) {
      dictionary = _generate_dictionary(values);
    }

    /**
     * Here begins the LZ4 stream compression. The library provides a function to create a stream which is used with
     * every new block that is to be compressed, but returns a raw pointer to an internal structure. The stream memory
     * is freed with another call to a library function after compression is done.
     */
    auto lz4_stream = LZ4_createStreamHC();
    // We use the maximum high compression level available in LZ4 for best compression ratios.
    LZ4_resetStreamHC(lz4_stream, LZ4HC_CLEVEL_MAX);

    auto lz4_blocks = pmr_vector<pmr_vector<char>>{alloc};
    auto num_blocks = input_size / _block_size;
    // Only add the last not-full block if the data doesn't perfectly fit into the block size.
    if (input_size % _block_size) {
      num_blocks++;
    }
    lz4_blocks.reserve(num_blocks);

    size_t total_compressed_size = 0u;
    size_t last_block_size = 0u;

    for (size_t block_index = 0u; block_index < num_blocks; ++block_index) {
      auto decompressed_block_size = _block_size;
      // The last block's uncompressed size varies.
      if (block_index + 1 == num_blocks) {
        decompressed_block_size = input_size - (block_index * _block_size);
        last_block_size = decompressed_block_size;
      }
      // LZ4_compressBound returns an upper bound for the size of the compressed data
      const auto block_bound = static_cast<size_t>(LZ4_compressBound(static_cast<int>(decompressed_block_size)));
      auto compressed_block = pmr_vector<char>{alloc};
      compressed_block.resize(block_bound);

      /**
       * If we previously learned a dictionary we use it to initialize LZ4 compression stream. Otherwise LZ4 uses the
       * previously compressed blocks instead, which would cause the blocks to depend on one another when decompressing.
       */
      if (!dictionary.empty()) {
        LZ4_loadDictHC(lz4_stream, dictionary.data(), static_cast<int>(dictionary.size()));
      }

      // The offset in the source data where the current block starts.
      const auto value_offset = block_index * _block_size;
      // move pointer to start position and pass to the actual compression method
      const int compression_result = LZ4_compress_HC_continue(
          lz4_stream, reinterpret_cast<char*>(values.data()) + value_offset, compressed_block.data(),
          static_cast<int>(decompressed_block_size), static_cast<int>(block_bound));

      Assert(compression_result > 0, "LZ4 stream compression failed");

      total_compressed_size += static_cast<size_t>(compression_result);

      // shrink the block vector to the actual size of the compressed result
      compressed_block.resize(static_cast<size_t>(compression_result));
      compressed_block.shrink_to_fit();

      lz4_blocks.emplace_back(std::move(compressed_block));
    }

    // Finally, release the LZ4 stream memory.
    LZ4_freeStreamHC(lz4_stream);

    return std::allocate_shared<LZ4Segment<T>>(alloc, std::move(lz4_blocks), std::move(null_values),
                                               std::move(dictionary), _block_size, last_block_size,
                                               total_compressed_size);
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const std::shared_ptr<const ValueSegment<pmr_string>>& value_segment) {
    const auto alloc = value_segment->values().get_allocator();
    const auto num_elements = value_segment->size();

    /**
     * First iterate over the values for two reasons.
     * 1) If all the strings are empty LZ4 will try to compress an empty vector which will cause a segmentation fault.
     *    In this case we can and need to do an early exit.
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

    /**
     * These are the lengths of each string. They are needed to train the zstd dictionary.
     */
    auto sample_sizes = pmr_vector<size_t>{alloc};
    sample_sizes.resize(num_elements);

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
        size_t sample_size;
        if (!is_null) {
          auto data = segment_value.value();
          values.insert(values.cend(), data.begin(), data.end());
          offset += data.size();
          sample_size = data.size();
        }
        sample_sizes[row_index] = sample_size;
      }
    });

    /**
     * If the input only contained null values and/or empty strings we don't need to compress anything (and LZ4 will
     * cause an error). Therefore we can already return the encoded segment.
     */
    if (!num_chars) {
      auto empty_blocks = pmr_vector<pmr_vector<char>>{alloc};
      auto empty_dictionary = pmr_vector<char>{};
      return std::allocate_shared<LZ4Segment<pmr_string>>(alloc, std::move(empty_blocks), std::move(null_values),
                                                          std::move(empty_dictionary), std::move(offsets), _block_size,
                                                          0u, 0u);
    }

    /**
     * Use the LZ4 high compression stream API to compress the copied values. The data is separated into different
     * blocks that are compressed independently. To maintain a high compression ratio and indepdence of these blocks
     * we first use zstd to generate a dictionary with the whole column as input. LZ4 can use the dictionary "learned"
     * on the column to compress the data in blocks.
     *
     * The C-library LZ4 expects raw pointers as input and output. To avoid directly handling raw pointers we use
     * std::vectors as input and output. The input vector contains the block that needs to be compressed and the
     * output vector is allocated enough memory to contain the compression result. Via the .data() call we can supply
     * LZ4 with raw pointers to the memory the vectors use. These are cast to char-pointers since LZ4 expects char
     * pointers.
     */

    const auto input_size = values.size();

    // Only if the segment is compressed into more than block it makes sense to use a dictionary.
    auto dictionary = pmr_vector<char>{alloc};
    if (input_size > _block_size) {
      dictionary = _generate_string_dictionary(values, sample_sizes);
    }

    /**
     * Here begins the LZ4 compression. The library provides a function to create a stream which is used with
     * every new block that is to be compressed, but returns a raw pointer to an internal structure. The stream memory
     * is freed with another call to a library function after compression is done.
     */
    auto lz4_stream = LZ4_createStreamHC();
    // We use the maximum high compression level available in LZ4 for best compression ratios.
    LZ4_resetStreamHC(lz4_stream, LZ4HC_CLEVEL_MAX);

    auto lz4_blocks = pmr_vector<pmr_vector<char>>{alloc};
    auto num_blocks = input_size / _block_size;
    // Only add the last not-full block if the data doesn't perfectly fit into the block size.
    if (input_size % _block_size) {
      num_blocks++;
    }
    lz4_blocks.reserve(num_blocks);

    size_t total_compressed_size = 0u;
    size_t last_block_size = 0u;

    for (size_t block_index = 0u; block_index < num_blocks; ++block_index) {
      auto decompressed_block_size = _block_size;
      // The last block's uncompressed size varies.
      if (block_index + 1 == num_blocks) {
        decompressed_block_size = input_size - (block_index * _block_size);
        last_block_size = decompressed_block_size;
      }
      // LZ4_compressBound returns an upper bound for the size of the compressed data
      const auto block_bound = static_cast<size_t>(LZ4_compressBound(static_cast<int>(decompressed_block_size)));
      auto compressed_block = pmr_vector<char>{alloc};
      compressed_block.resize(block_bound);

      /**
       * If we previously learned a dictionary we use it to initialize LZ4. Otherwise LZ4 uses the previously
       * compressed block instead, which would cause the blocks to depend on one another.
       */
      if (!dictionary.empty()) {
        LZ4_loadDictHC(lz4_stream, dictionary.data(), static_cast<int>(dictionary.size()));
      }

      // The offset in the source data where the current block starts.
      const auto value_offset = block_index * _block_size;
      // move pointer to start position and pass to the actual compression method
      const int compression_result = LZ4_compress_HC_continue(
          lz4_stream, reinterpret_cast<char*>(values.data()) + value_offset, compressed_block.data(),
          static_cast<int>(decompressed_block_size), static_cast<int>(block_bound));

      Assert(compression_result > 0, "LZ4 stream compression failed");

      total_compressed_size += static_cast<size_t>(compression_result);

      // shrink the block vector to the actual size of the compressed result
      compressed_block.resize(static_cast<size_t>(compression_result));
      compressed_block.shrink_to_fit();

      lz4_blocks.emplace_back(std::move(compressed_block));
    }

    // Finally, release the LZ4 stream memory.
    LZ4_freeStreamHC(lz4_stream);

    return std::allocate_shared<LZ4Segment<pmr_string>>(alloc, std::move(lz4_blocks), std::move(null_values),
                                                        std::move(dictionary), std::move(offsets), _block_size,
                                                        last_block_size, total_compressed_size);
  }

 private:
  template <typename T>
  pmr_vector<char> _generate_dictionary(const pmr_vector<T>& values) {
    const auto num_values = values.size();
    const auto values_size = num_values * sizeof(T);
    // 8 bytes is the minimum size of a sample
    const auto sample_size = sizeof(T) > 8 ? sizeof(T) : 8;
    const auto num_samples = values_size / sample_size;
    // all samples are of the same size
    const std::vector<size_t> sample_lens(num_samples, sample_size);
    // The recommended dictionary size is about 1/100th of size of all samples combined.
    auto max_dictionary_size = num_samples * sample_size / 100;
    // But the size also has to be at least 1KB (smaller dictionaries won't work).
    max_dictionary_size = max_dictionary_size < 1000u ? 1000u : max_dictionary_size;

    auto dictionary = pmr_vector<char>{values.get_allocator()};
    dictionary.resize(max_dictionary_size);

    const auto dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values.data(),
                                                       sample_lens.data(), static_cast<unsigned>(sample_lens.size()));
    Assert(!ZDICT_isError(dictionary_size), "ZSTD dictionary generation failed in LZ4 compression.");
    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }

  pmr_vector<char> _generate_string_dictionary(const pmr_vector<char>& values, const pmr_vector<size_t>& sample_sizes) {
    std::cout << "Building dictionary" << std::endl;
    const auto num_values = values.size();
    // The recommended dictionary size is about 1/100th of size of all samples combined.
    auto max_dictionary_size = num_values / 100;
    // But the size also has to be at least 1KB (smaller dictionaries won't work).
    max_dictionary_size = max_dictionary_size < 1000u ? 1000u : max_dictionary_size;

    pmr_vector<char> dictionary;
//    auto dictionary = pmr_vector<char>{values.get_allocator()};
//    dictionary.resize(max_dictionary_size);

    size_t dictionary_size;
    auto values_copy = pmr_vector<char>{};
    auto samples_copy = pmr_vector<size_t>{};
    do {
      std::cout << "Dictionary max size: " << max_dictionary_size << std::endl;
      max_dictionary_size = max_dictionary_size < 1000u ? 1000u : max_dictionary_size / 2;

      dictionary = pmr_vector<char>{values.get_allocator()};
      dictionary.resize(max_dictionary_size);

      values_copy.insert(values_copy.end(), values.begin(), values.end());
      values_copy.insert(values_copy.end(), values.begin(), values.end());
      samples_copy.emplace_back(values.size());
      for (size_t index = 0; index < sample_sizes.size(); index += 2) {
        if (index + 1 < sample_sizes.size()) {
          samples_copy.emplace_back(sample_sizes[index] + sample_sizes[index + 1]);
        } else {
          samples_copy.emplace_back(sample_sizes[index]);
        }
      }
//      samples_copy.insert(samples_copy.end(), sample_sizes.begin(), sample_sizes.end());
      std::cout << "Trying dictionary with " << values_copy.size() << " values" << std::endl;
      dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values_copy.data(),
                                              samples_copy.data(), static_cast<unsigned>(samples_copy.size()));

    } while (ZDICT_isError(dictionary_size));
    std::cout << "Success with " << values_copy.size() << " values" << std::endl;

//    const auto dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values.data(),
//                                                       sample_sizes.data(), static_cast<unsigned>(sample_sizes.size()));
    Assert(!ZDICT_isError(dictionary_size), "ZSTD dictionary generation failed in LZ4 compression.");
    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }
};

}  // namespace opossum
