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
#include "storage/vector_compression/simd_bp128/simd_bp128_compressor.hpp"
#include "storage/vector_compression/simd_bp128/simd_bp128_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

/**
 * This encoder compresses a value segment with the LZ4 library. LZ4 allows two different modes: block and stream
 * compression.
 *
 * Block compression compresses the segment into one large blob. To access any value of the segment, the
 * whole blob has to be decompressed first. This causes a large overhead for random access into the segment.
 *
 * Stream compression is made for compressing large files as a stream of blocks, while still taking advantage of
 * information redundancy in the whole file - single block compression can't do that and thus can not compress with
 * as good ratios. LZ4 builds up a dictionary internally that is incremented with every block that is compressed.
 * In consequence of that, those blocks can only be decompressed in the same order they were compressed, making it
 * useless for our point access case, where we only want to decompress the one block containing the requested element
 * (or several blocks for strings larger than one block size).
 * To circumvent that we can use a pre-trained dictionary: instead of building up a dictionary internally, it is
 * already provided for compression and decompression. That makes it possible decompress a block independently.
 *
 * A value segment is split into multiple blocks that are compressed with the LZ4 stream compression mode. The
 * pre-trained dictionary is created with the zstd-library that uses all values in the segment to train the dictionary.
 * This training can fail if there is not enough input data. In that case, the data is still split into blocks, but
 * these are compressed independently. That means that LZ4 can't use any information redundancy between these blocks and
 * therefore, the compression ratio will suffer.
 * In the case that the input data fits into a single block, this block is compressed without training a dictionary
 */
class LZ4Encoder : public SegmentEncoder<LZ4Encoder> {
 public:
  static constexpr auto _encoding_type = enum_c<EncodingType, EncodingType::LZ4>;
  static constexpr auto _uses_vector_compression = true;
  /**
   * A block size of 16 KB was chosen, since the recommended minimal amount of data to train a zstd dictionary is around
   * 20 KB. Therefore, there is no point in trying to train a dictionary with less data than that (and the training
   * fails sometimes in the edge case of two blocks in the segment with the total amount of data being still less than
   * 20 KB).
   * With only one block in a segment there is no need for a zstd dictionary. With multiple blocks but no dictionary
   * (due to not enough data) the compression ratio suffers, since LZ4 can only view and compress small amounts of data
   * at once (refer to the comment above).
   */
  static constexpr auto _block_size = size_t{16384u};
  static_assert(_block_size <= size_t{std::numeric_limits<int>::max()},
                "LZ4 block size can't be larger than the maximum value of a 32 bit signed int");

  template <typename T>
  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<T> segment_iterable,
                                                 const PolymorphicAllocator<T>& allocator) {
    // TODO(anyone): when value segments switch to using pmr_vectors, the data can be copied directly instead of
    // copying it element by element
    auto values = pmr_vector<T>{allocator};
    auto null_values = pmr_vector<bool>{allocator};

    /**
     * If the null value vector only contains the value false, then the value segment does not have any row value that
     * is null. In that case, we don't store the null value vector to reduce the LZ4 segment's memory footprint.
     */
    auto segment_contains_null = false;

    segment_iterable.with_iterators([&](auto it, auto end) {
      const auto segment_size = static_cast<size_t>(std::distance(it, end));
      values.resize(segment_size);
      null_values.resize(segment_size);

      // iterate over the segment to access the values and increment the row index to copy values and null flags
      auto row_index = size_t{0u};
      for (; it != end; ++it) {
        const auto segment_value = *it;
        const auto contains_null = segment_value.is_null();
        values[row_index] = segment_value.value();
        null_values[row_index] = contains_null;
        segment_contains_null = segment_contains_null || contains_null;

        ++row_index;
      }
    });

    auto optional_null_values = segment_contains_null ? std::optional<pmr_vector<bool>>{null_values} : std::nullopt;

    /**
     * Pre-compute a zstd dictionary if the input data is split among multiple blocks. This dictionary allows
     * independent compression of the blocks, while maintaining a good compression ratio.
     * If the input data fits into a single block, training of a dictionary is skipped.
     */
    const auto input_size = values.size() * sizeof(T);
    auto dictionary = pmr_vector<char>{};
    if (input_size > _block_size) {
      dictionary = _train_dictionary(values);
    }

    /**
     * Compress the data and calculate the last block size (which may vary from the block size of the previous blocks)
     * and the total compressed size. The size of the last block is needed for decompression. The total compressed
     * size is pre-calculated instead of iterating over all blocks when the memory consumption of the LZ4 segment is
     * estimated.
     */
    auto lz4_blocks = pmr_vector<pmr_vector<char>>{allocator};
    auto total_compressed_size = size_t{0u};
    auto last_block_size = size_t{0u};
    if (!values.empty()) {
      _compress(values, lz4_blocks, dictionary);
      last_block_size = input_size % _block_size != 0 ? input_size % _block_size : _block_size;
      for (const auto& compressed_block : lz4_blocks) {
        total_compressed_size += compressed_block.size();
      }
    }

    return std::allocate_shared<LZ4Segment<T>>(allocator, std::move(lz4_blocks), std::move(optional_null_values),
                                               std::move(dictionary), _block_size, last_block_size,
                                               total_compressed_size, values.size());
  }

  std::shared_ptr<BaseEncodedSegment> _on_encode(const AnySegmentIterable<pmr_string> segment_iterable,
                                                 const PolymorphicAllocator<pmr_string>& allocator) {
    /**
     * First iterate over the values for two reasons.
     * 1) If all the strings are empty LZ4 will try to compress an empty vector which will cause a segmentation fault.
     *    In this case we can and need to do an early exit.
     * 2) Sum the length of the strings to improve the performance when copying the data to the char vector.
     */
    auto num_chars = size_t{0u};
    segment_iterable.with_iterators([&](auto it, auto end) {
      for (; it != end; ++it) {
        if (!it->is_null()) {
          num_chars += it->value().size();
        }
      }
    });

    // copy values and null flags from value segment
    auto values = pmr_vector<char>{allocator};
    values.reserve(num_chars);
    auto null_values = pmr_vector<bool>{allocator};

    /**
     * If the null value vector only contains the value false, then the value segment does not have any row value that
     * is null. In that case, we don't store the null value vector to reduce the LZ4 segment's memory footprint.
     */
    auto segment_contains_null = false;

    /**
     * These offsets mark the beginning of strings (and therefore end of the previous string) in the data vector.
     * These offsets are character offsets. The string at position 0 starts at the offset stored at position 0, which
     * will always be 0.
     * Its exclusive end is the offset stored at position 1 (i.e., offsets[1] - 1 is the last character of the string
     * at position 0).
     * In case of the last string its end is determined by the end of the data vector.
     *
     * The offsets are stored as 32 bit unsigned integer as opposed to 64 bit (size_t) so that they can later be
     * compressed via vector compression.
     */
    auto offsets = pmr_vector<uint32_t>{allocator};

    /**
     * These are the lengths of each string. They are needed to train the zstd dictionary.
     */
    auto string_samples_lengths = pmr_vector<size_t>{allocator};

    segment_iterable.with_iterators([&](auto it, auto end) {
      const auto segment_size = std::distance(it, end);

      null_values.resize(segment_size);
      offsets.resize(segment_size);
      string_samples_lengths.resize(segment_size);

      auto offset = uint32_t{0u};
      // iterate over the iterator to access the values and increment the row index to write to the values and null
      // values vectors
      auto row_index = size_t{0};
      for (; it != end; ++it) {
        const auto segment_element = *it;
        const auto contains_null = segment_element.is_null();
        null_values[row_index] = contains_null;
        segment_contains_null = segment_contains_null || contains_null;
        offsets[row_index] = offset;
        auto sample_size = size_t{0u};
        if (!contains_null) {
          const auto value = segment_element.value();
          const auto string_length = value.size();
          values.insert(values.cend(), value.begin(), value.end());
          Assert(string_length <= std::numeric_limits<uint32_t>::max(),
                 "The size of string row value exceeds the maximum of uint32 in LZ4 encoding.");
          offset += static_cast<uint32_t>(string_length);
          sample_size = string_length;
        }

        string_samples_lengths[row_index] = sample_size;
        ++row_index;
      }
    });

    auto optional_null_values = segment_contains_null ? std::optional<pmr_vector<bool>>{null_values} : std::nullopt;

    /**
     * If the input only contained null values and/or empty strings we don't need to compress anything (and LZ4 will
     * cause an error). We can also throw away the offsets, since they won't be used for decompression.
     * We can do an early exit and return the (not encoded) segment.
     */
    if (num_chars == 0) {
      auto empty_blocks = pmr_vector<pmr_vector<char>>{allocator};
      auto empty_dictionary = pmr_vector<char>{};
      return std::allocate_shared<LZ4Segment<pmr_string>>(allocator, std::move(empty_blocks),
                                                          std::move(optional_null_values), std::move(empty_dictionary),
                                                          nullptr, _block_size, 0u, 0u, null_values.size());
    }

    // Compress the offsets with SimdBp128 vector compression to reduce the memory footprint of the LZ4 segment.
    // SimdBp128 is chosen over fixed size byte-aligned (FSBA) vector compression, since it compresses better and the
    // performance advantage of FSBA is neglectable, because runtime is dominated by the LZ4 encoding/decoding anyways.
    auto compressed_offsets = compress_vector(offsets, VectorCompressionType::SimdBp128, allocator, {offsets.back()});

    /**
     * Pre-compute a zstd dictionary if the input data is split among multiple blocks. This dictionary allows
     * independent compression of the blocks, while maintaining a good compression ratio.
     * If the input data fits into a single block, training of a dictionary is skipped.
     */
    const auto input_size = values.size();
    auto dictionary = pmr_vector<char>{allocator};
    if (input_size > _block_size) {
      dictionary = _train_dictionary(values, string_samples_lengths);
    }

    /**
     * Compress the data and calculate the last block size (which may vary from the block size of the previous blocks)
     * and the total compressed size. The size of the last block is needed for decompression. The total compressed size
     * is pre-calculated instead of iterating over all blocks when the memory consumption of the LZ4 segment is
     * estimated.
     */
    auto lz4_blocks = pmr_vector<pmr_vector<char>>{allocator};
    _compress(values, lz4_blocks, dictionary);

    auto last_block_size = input_size % _block_size != 0 ? input_size % _block_size : _block_size;

    auto total_compressed_size = size_t{0u};
    for (const auto& compressed_block : lz4_blocks) {
      total_compressed_size += compressed_block.size();
    }

    return std::allocate_shared<LZ4Segment<pmr_string>>(
        allocator, std::move(lz4_blocks), std::move(optional_null_values), std::move(dictionary),
        std::move(compressed_offsets), _block_size, last_block_size, total_compressed_size, null_values.size());
  }

 private:
  static constexpr auto _minimum_dictionary_size = size_t{1000u};
  static constexpr auto _minimum_value_size = size_t{20000u};

  /**
   * Use the LZ4 high compression stream API to compress the input values. The data is separated into different
   * blocks that are compressed independently. To maintain a high compression ratio and independence of these blocks
   * we use the dictionary trained via zstd. LZ4 can use the dictionary "learned" on the column to compress the data
   * in blocks independently while maintaining a good compression ratio.
   *
   * The C-library LZ4 needs raw pointers as input and output. To avoid directly handling raw pointers, we use
   * std::vectors as input and output. The input vector contains the block that needs to be compressed and the
   * output vector has allocated enough memory to contain the compression result. Via the .data() call we can supply
   * LZ4 with raw pointers to the memory the vectors use. These are cast to char-pointers since LZ4 expects char
   * pointers.
   *
   * @tparam T The type of the input data. In the case of non-string-segments, this is the segment type. In the case of
   *           string-segments, this will be char.
   * @param values The values that are compressed.
   * @param lz4_blocks The vector to which the generated LZ4 blocks are appended.
   * @param dictionary The dictionary trained via zstd. If this dictionary is empty, the blocks are still compressed
   *                   independently but the compression ratio might suffer.
   */
  template <typename T>
  void _compress(pmr_vector<T>& values, pmr_vector<pmr_vector<char>>& lz4_blocks, const pmr_vector<char>& dictionary) {
    /**
     * Here begins the LZ4 compression. The library provides a function to create a stream. The stream is used with
     * every new block that is to be compressed, but the stream returns a raw pointer to an internal structure.
     * The stream memory is freed with another call to a library function after compression is done.
     */
    auto lz4_stream = LZ4_createStreamHC();
    // We use the maximum high compression level available in LZ4 for best compression ratios.
    LZ4_resetStreamHC(lz4_stream, LZ4HC_CLEVEL_MAX);

    const auto input_size = values.size() * sizeof(T);
    auto num_blocks = input_size / _block_size;
    // Only add the last not-full block if the data doesn't perfectly fit into the block size.
    if (input_size % _block_size != 0) {
      num_blocks++;
    }
    lz4_blocks.reserve(num_blocks);

    for (auto block_index = size_t{0u}; block_index < num_blocks; ++block_index) {
      auto decompressed_block_size = _block_size;
      // The last block's uncompressed size varies.
      if (block_index + 1 == num_blocks) {
        decompressed_block_size = input_size - (block_index * _block_size);
      }
      // LZ4_compressBound returns an upper bound for the size of the compressed data
      const auto block_bound = static_cast<size_t>(LZ4_compressBound(static_cast<int>(decompressed_block_size)));
      auto compressed_block = pmr_vector<char>{values.get_allocator()};
      compressed_block.resize(block_bound);

      /**
       * If we previously learned a dictionary, we use it to initialize LZ4. Otherwise LZ4 uses the previously
       * compressed block instead, which would cause the blocks to depend on one another.
       * If we have no dictionary present and compress at least a second block (i.e., block_index > 0), then we reset
       * the LZ4 stream to maintain the independence of the blocks. This only happens when the column does not contain
       * enough data to produce a zstd dictionary (i.e., a column of single character strings).
       */
      if (!dictionary.empty()) {
        LZ4_loadDictHC(lz4_stream, dictionary.data(), static_cast<int>(dictionary.size()));
      } else if (block_index) {
        LZ4_resetStreamHC(lz4_stream, LZ4HC_CLEVEL_MAX);
      }

      // The offset in the source data where the current block starts.
      const auto value_offset = block_index * _block_size;
      // move pointer to start position and pass to the actual compression method
      const int compression_result = LZ4_compress_HC_continue(
          lz4_stream, reinterpret_cast<char*>(values.data()) + value_offset, compressed_block.data(),
          static_cast<int>(decompressed_block_size), static_cast<int>(block_bound));

      Assert(compression_result > 0, "LZ4 stream compression failed");

      // shrink the block vector to the actual size of the compressed result
      compressed_block.resize(static_cast<size_t>(compression_result));
      compressed_block.shrink_to_fit();

      lz4_blocks.emplace_back(std::move(compressed_block));
    }

    // Finally, release the LZ4 stream memory.
    LZ4_freeStreamHC(lz4_stream);
  }

  /**
   * Train a zstd dictionary. This dictionary is used to compress different lz4 blocks independently, while maintaining
   * a high compression ratio. The independent compression of the blocks is necessary to decompress them independently,
   * which allows for efficient random access.
   *
   * This method should be called for non-string data, since each value has the same size. The zstd dictionary is
   * originally intended for string data, in which each sample can have a different size.
   * Non-string data types have a constant size and can be smaller than the minimum sample size of 8 bytes. Therefore,
   * the sample sizes are the same for each sample and if the size of the data type is less than the minimum sample
   * size, multiple values are a single sample (e.g., 2 values for 32 bit integers).
   *
   * @tparam T The data type of the value segment. This method should only be called for non-string-segments.
   * @param values All values of the segment. They are the input data to train the dictionary.
   * @return The trained dictionary, or in the case of failure, an empty vector.
   */
  template <typename T>
  pmr_vector<char> _train_dictionary(const pmr_vector<T>& values) {
    const auto min_sample_size = size_t{8u};
    const auto values_size = values.size() * sizeof(T);
    const auto sample_size = std::max(sizeof(T), min_sample_size);
    const auto num_samples = values_size / sample_size;
    const auto sample_sizes = pmr_vector<size_t>(num_samples, sample_size);

    return _train_dictionary(values, sample_sizes);
  }

  /**
   * Train a zstd dictionary. In the case of non-string data, the sample sizes are the same. In the case of string data,
   * each row element is a sample (with differing sizes).
   * If the dictionary training fails, a dictionary can't be used for compression. Since the blocks have to be
   * compressed independently for independent access, the compression ratio will suffer.
   *
   * @tparam T The data type of the value segment.
   * @param values The input data that will be compressed (i.e., all rows concatenated).
   * @param sample_sizes A vector of sample lengths. In the case of strings, each length corresponds to a substring in
   *                     the values vector. These should correspond to the length of each row's value.
   *                     In the case of non-strings these will all have the same value and correspond byte blocks,
   *                     possibly containing more than one row value.
   * @return The trained dictionary or in the case of failure an empty vector.
   */
  template <typename T>
  pmr_vector<char> _train_dictionary(const pmr_vector<T>& values, const pmr_vector<size_t>& sample_sizes) {
    /**
     * The recommended dictionary size is about 1/100th of size of all samples combined, but the size also has to be at
     * least 1KB. Smaller dictionaries won't work.
     */
    auto max_dictionary_size = values.size() / 100;
    max_dictionary_size = std::max(max_dictionary_size, _minimum_dictionary_size);

    auto dictionary = pmr_vector<char>{values.get_allocator()};
    size_t dictionary_size;

    // If the input does not contain enough values, it won't be possible to train a dictionary for it.
    if (values.size() < _minimum_value_size) {
      return dictionary;
    }

    dictionary.resize(max_dictionary_size);
    dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values.data(), sample_sizes.data(),
                                            static_cast<unsigned>(sample_sizes.size()));

    // If the generation failed, then compress without a dictionary (the compression ratio will suffer).
    if (ZDICT_isError(dictionary_size)) {
      return pmr_vector<char>{};
    }

    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");

    // Shrink the allocated dictionary size to the actual size.
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }
};

}  // namespace opossum
