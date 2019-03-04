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
  static constexpr size_t _block_size = 16384u;
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
     * Pre-compute a zstd dictionary if the input data is split among multiple blocks. This dictionary allows indepdent
     * compression of the blocks, while maintaining a good compression ratio.
     */
    const auto input_size = values.size() * sizeof(T);
    auto dictionary = pmr_vector<char>{};
    if (input_size > _block_size) {
      dictionary = _generate_dictionary(values);
    }

    /**
     * Compress the data and calculate the last block size (which may vary from the block size) and the total compressed
     * size. The size of the last block is needed for decompression and the total compressed size is pre-calculated
     * instead of iterating over all blocks when the memory consumption of the LZ4 segment is estimated.
     */
    auto lz4_blocks = pmr_vector<pmr_vector<char>>{alloc};
    _compress(values, lz4_blocks, dictionary);

    auto last_block_size = input_size % _block_size ? input_size % _block_size : _block_size;

    size_t total_compressed_size{};
    for (const auto& block : lz4_blocks) {
      total_compressed_size += block.size();
    }

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
     * Pre-compute a zstd dictionary if the input data is split among multiple blocks. This dictionary allows indepdent
     * compression of the blocks, while maintaining a good compression ratio.
     */
    const auto input_size = values.size();
    auto dictionary = pmr_vector<char>{alloc};
    if (input_size > _block_size) {
      dictionary = _generate_string_dictionary(values, sample_sizes);
    }

    /**
     * Compress the data and calculate the last block size (which may vary from the block size) and the total compressed
     * size. The size of the last block is needed for decompression and the total compressed size is pre-calculated
     * instead of iterating over all blocks when the memory consumption of the LZ4 segment is estimated.
     */
    auto lz4_blocks = pmr_vector<pmr_vector<char>>{alloc};
    _compress(values, lz4_blocks, dictionary);

    auto last_block_size = input_size % _block_size ? input_size % _block_size : _block_size;

    size_t total_compressed_size{};
    for (const auto& block : lz4_blocks) {
      total_compressed_size += block.size();
    }

    return std::allocate_shared<LZ4Segment<pmr_string>>(alloc, std::move(lz4_blocks), std::move(null_values),
                                                        std::move(dictionary), std::move(offsets), _block_size,
                                                        last_block_size, total_compressed_size);
  }

 private:
  static constexpr size_t _maximum_dictionary_size = 10000000u;
  static constexpr size_t _maximum_value_size = 1000000u;
  static constexpr size_t _minimum_value_size = 20000u;

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

  /**
   * Use the LZ4 high compression stream API to compress the input values. The data is separated into different
   * blocks that are compressed independently. To maintain a high compression ratio and indepdence of these blocks
   * we use dictionary generated via zstd. LZ4 can use the dictionary "learned" on the column to compress the data
   * in blocks independently while maintaining a good compression ratio.
   *
   * @tparam T The type of the input data. In the case of non-string segments this is the segment type. In the case of
   *           string segments this will be char.
   * @param values The values that are compressed.
   * @param lz4_blocks The vector to which the generated LZ4 blocks are appended.
   * @param dictionary The dictionary generated via zstd. If this dictionary is empty, the blocks are still compressed
   *                   indepdendently but the compression ratio might suffer.
   */
  template <typename T>
  void _compress(
    pmr_vector<T>& values, pmr_vector<pmr_vector<char>>& lz4_blocks, const pmr_vector<char>& dictionary) {
    /**
     * Here begins the LZ4 compression. The library provides a function to create a stream which is used with
     * every new block that is to be compressed, but returns a raw pointer to an internal structure. The stream memory
     * is freed with another call to a library function after compression is done.
     */
    auto lz4_stream = LZ4_createStreamHC();
    // We use the maximum high compression level available in LZ4 for best compression ratios.
    LZ4_resetStreamHC(lz4_stream, LZ4HC_CLEVEL_MAX);

    const auto input_size = values.size() * sizeof(T);
    auto num_blocks = input_size / _block_size;
    // Only add the last not-full block if the data doesn't perfectly fit into the block size.
    if (input_size % _block_size) {
      num_blocks++;
    }
    lz4_blocks.reserve(num_blocks);

    for (size_t block_index = 0u; block_index < num_blocks; ++block_index) {
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
       * If we previously learned a dictionary we use it to initialize LZ4. Otherwise LZ4 uses the previously
       * compressed block instead, which would cause the blocks to depend on one another.
       * If there is no dictionary present and we are compressing at least a second block (i.e. block_index > 0)
       * then we reset the LZ4 stream to maintain the independence of the blocks. This only happens when the column
       * does not contain enough data to produce a zstd dictionary (i.e., a column of single character strings).
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
   * Generate a dictionary for non-strings. The zstd dictionary is intended for string data. Therefore, the samples
   * provided to the zstd algorithm are not the values of each row but multiple values at once (since the minimum size
   * for a sample is 8 bytes).
   *
   * @tparam T The data type of the value segment. This method is only called for non-string segments.
   * @param values All values of the segment. They are the input data to train the dictionary.
   * @return The generated dictionary or in the case of failure an empty vector.
   */
  template <typename T>
  pmr_vector<char> _generate_dictionary(const pmr_vector<T>& values) {
    /**
     * The minimum sample size is 8 bytes. Since non-string data has a constant size for each value we can just set
     * the sample size to 8 for all values.
     */
    const auto values_size = values.size() * sizeof(T);
    const auto sample_size = sizeof(T) > 8 ? sizeof(T) : 8;
    const auto num_samples = values_size / sample_size;
    const std::vector<size_t> sample_lens(num_samples, sample_size);

    /**
     * The recommended dictionary size is about 1/100th of size of all samples combined, but he size also has to be at
     * least 1KB. Smaller dictionaries won't work.
     */
    auto max_dictionary_size = num_samples * sample_size / 100;
    max_dictionary_size = max_dictionary_size < 1000u ? 1000u : max_dictionary_size;

    auto dictionary = pmr_vector<char>{values.get_allocator()};
    dictionary.resize(max_dictionary_size);

    const auto dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values.data(),
                                                       sample_lens.data(), static_cast<unsigned>(sample_lens.size()));
    // If the generation failed, then compress without a dictionary (the compression ratio will suffer).
    if (ZDICT_isError(dictionary_size)) {
      return pmr_vector<char>{};
    }

    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }

  /**
   * When generating a dictionary for strings, each row element is a sample. This is not done when generating a
   * dictionary for non-strings since zstd's dictionary is made for strings (and the samples can have different sizes).
   * First, a dictionary is trained with the provided data and sample sizes. If this does not succeed, we try to
   * increase the input size by repeating the values and adding larger sample sizes up to a certain limit. If this still
   * fails or the input size is too small in general, a dictionary won't be generated and can't be used for compression.
   * To maintain block independence is that case the compression ratio will suffer.
   *
   * @param values The input data that will be compressed (i.e. all strings concatenated).
   * @param sample_sizes A vector of sample lengths. Each length corresponds to a substring in the values vector. These
   *                     should correspond to the length of each row's value.
   * @return The generated dictionary or in the case of failure an empty vector.
   */
  pmr_vector<char> _generate_string_dictionary(const pmr_vector<char>& values, const pmr_vector<size_t>& sample_sizes) {
    /**
     * The recommended dictionary size is about 1/100th of size of all samples combined, but he size also has to be at
     * least 1KB. Smaller dictionaries won't work.
     */
    auto max_dictionary_size = values.size() / 100;
    max_dictionary_size = max_dictionary_size < 1000u ? 1000u : max_dictionary_size;

    auto dictionary = pmr_vector<char>{values.get_allocator()};
    size_t dictionary_size;

    // If the input does not contain enough values, it won't be possible to generate a dictionary for it.
    if (values.size() < _minimum_value_size) {
      return dictionary;
    }

    dictionary.resize(max_dictionary_size);
    dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values.data(),
                                            sample_sizes.data(), static_cast<unsigned>(sample_sizes.size()));

    // If the generation failed, try generating a dictionary with more input.
    if (ZDICT_isError(dictionary_size)) {
      return _generate_string_dictionary_padded(values, sample_sizes, max_dictionary_size);
    }

    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");

    // Shrink the allocated dictionary size to the actual size.
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }

  /**
   * Increase the dictionary input data by appending the original input data (linear increase) and increasing the
   * maximum dictionary size (exponential increase). This only happens if the input data is too small for zstd to
   * successfully generate a dictionary.
   *
   * The values and dictionary are increased up to a certain threshold (_maximum_dictionary_size and
   * _maximum_value_size). If the dictionary generation still fails at that point, it is aborted and the compression
   * will continue without a dictionary.
   *
   * @param values Vector that contains the input data for the dictionary generation.
   * @param sample_sizes A vector of sample lengths. Each length corresponds to a substring in the values vector. These
   *                     should correspond to the length of each row's value.
   * @param max_dictionary_size_estimate the original estimate for the maximum dictionary size
   * @return The generated dictionary or in the case of failure an empty vector.
   */
  pmr_vector<char> _generate_string_dictionary_padded(
    const pmr_vector<char>& values, const pmr_vector<size_t>& sample_sizes, const size_t max_dictionary_size_estimate) {

    pmr_vector<char> dictionary;
    size_t dictionary_size;
    size_t max_dictionary_size = max_dictionary_size_estimate;

    /**
     * Work on copies of the input data and sample sizes. These vectors will increase in size by repeatedly appending
     * the original input data and larger sample sizes.
     */
    auto values_copy = pmr_vector<char>{values, values.get_allocator()};
    auto sample_sizes_copy = pmr_vector<size_t>{sample_sizes, sample_sizes.get_allocator()};

    do {
      /**
       * This method is only called when the dictionary generation with the input data failed. Therefore, we start by
       * increasing the input data linearly.
       */
      dictionary = pmr_vector<char>{values.get_allocator()};
      max_dictionary_size = std::min(2u * max_dictionary_size, _maximum_dictionary_size);
      dictionary.resize(max_dictionary_size);
      _increase_dictionary_input_data(values_copy, sample_sizes_copy, values.size());

      dictionary_size = ZDICT_trainFromBuffer(dictionary.data(), max_dictionary_size, values_copy.data(),
                                              sample_sizes_copy.data(), static_cast<unsigned>(sample_sizes_copy.size()));

    } while (ZDICT_isError(dictionary_size) && max_dictionary_size < _maximum_dictionary_size
             && values_copy.size() < _maximum_value_size);

    /**
     * If the generation still failed with increased input data, then compress without a dictionary (the compression
     * ratio will suffer).
     */
    if (ZDICT_isError(dictionary_size)) {
      return pmr_vector<char>{};
    }

    DebugAssert(dictionary_size <= max_dictionary_size,
                "Generated ZSTD dictionary in LZ4 compression is larger than "
                "the memory allocated for it.");
    dictionary.resize(dictionary_size);
    dictionary.shrink_to_fit();

    return dictionary;
  }

  /**
   * Increase the dictionary input data by appending the original input data (linear increase). This only happens if
   * the input data is too small for zstd to successfully generate a dictionary.
   *
   * The sample sizes are not copied directly. First we add a sample size of the whole input vector (somehow this is
   * important for zstd to sucessfully generate a dictionary).
   * Afterwards we again add the whole range of the input data as sample sizes of size 10 (and the last sample size
   * varies according to the size of the input data).
   *
   * The input data and sample sizes should have to correspond to each other perfectly (i.e., the sum of all sample
   * sizes should be the length of the value vector), but somehow this works (and not adding the size of the values
   * vector as sample size causes zstd to fail at generating a dictionary).
   *
   * @param values Vector that contains the input data for the dictionary generation. Contains the input data repeated
   *               n times (i.e., its size equals n * num_values). This method appends items to this vector.
   * @param sample_sizes The sample sizes provided so zstd. This method appends items to this vector.
   * @param num_values The number of characters in the original data.
   */
  void _increase_dictionary_input_data(pmr_vector<char>& values, pmr_vector<size_t>& sample_sizes, size_t num_values) {
    // The first num_values values in the value data is the original input. It is just copied and appended again.
    values.insert(values.end(), values.begin(), values.begin() + num_values);

    // This magic line is needed for zstd to suceed.
    sample_sizes.emplace_back(num_values);

    /**
     * Also add the whole data range in samples of size 10 to the sample size vector. This is also needed in combination
     * with the line above. The idea here, is too provide larger samples to zstd. The input data is only too small to
     * generate a dictionary when the strings are very short (i.e., single character values).
     */
    for (size_t i = 0u; i < num_values; i += 10u) {
      if (i + 10u < num_values) {
        sample_sizes.emplace_back(10u);
      } else {
        sample_sizes.emplace_back(num_values - i);
      }
    }
  }
};

}  // namespace opossum
