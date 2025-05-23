#include "lz4_segment.hpp"

#include <climits>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "lz4.h"

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_encoded_segment.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, std::optional<pmr_vector<bool>>&& null_values,
                          pmr_vector<char>&& dictionary, const size_t block_size, const size_t last_block_size,
                          const size_t compressed_size, const size_t num_elements)
    : AbstractEncodedSegment{data_type_from_type<T>()},
      _lz4_blocks{std::move(lz4_blocks)},
      _null_values{std::move(null_values)},
      _dictionary{std::move(dictionary)},
      _string_offsets{nullptr},
      _block_size{block_size},
      _last_block_size{last_block_size},
      _compressed_size{compressed_size},
      _num_elements{num_elements} {}

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, std::optional<pmr_vector<bool>>&& null_values,
                          pmr_vector<char>&& dictionary, std::unique_ptr<const BaseCompressedVector>&& string_offsets,
                          const size_t block_size, const size_t last_block_size, const size_t compressed_size,
                          const size_t num_elements)
    : AbstractEncodedSegment{data_type_from_type<T>()},
      _lz4_blocks{std::move(lz4_blocks)},
      _null_values{std::move(null_values)},
      _dictionary{std::move(dictionary)},
      _string_offsets{std::move(string_offsets)},
      _block_size{block_size},
      _last_block_size{last_block_size},
      _compressed_size{compressed_size},
      _num_elements{num_elements} {}

template <typename T>
AllTypeVariant LZ4Segment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
std::optional<T> LZ4Segment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (_null_values && (*_null_values)[chunk_offset]) {
    return std::nullopt;
  }

  return decompress(chunk_offset);
}

template <typename T>
const std::optional<pmr_vector<bool>>& LZ4Segment<T>::null_values() const {
  return _null_values;
}

template <typename T>
std::unique_ptr<BaseVectorDecompressor> LZ4Segment<T>::string_offset_decompressor() const {
  if (_string_offsets) {
    return _string_offsets->create_base_decompressor();
  }

  return nullptr;
}

template <typename T>
const pmr_vector<char>& LZ4Segment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
ChunkOffset LZ4Segment<T>::size() const {
  return static_cast<ChunkOffset>(_num_elements);
}

template <typename T>
const pmr_vector<pmr_vector<char>>& LZ4Segment<T>::lz4_blocks() const {
  return _lz4_blocks;
}

template <typename T>
size_t LZ4Segment<T>::block_size() const {
  return _block_size;
}

template <typename T>
size_t LZ4Segment<T>::last_block_size() const {
  return _last_block_size;
}

template <typename T>
const std::unique_ptr<const BaseCompressedVector>& LZ4Segment<T>::string_offsets() const {
  return _string_offsets;
}

template <typename T>
std::vector<T> LZ4Segment<T>::decompress() const {
  auto decompressed_data = std::vector<T>(size());

  const auto num_blocks = _lz4_blocks.size();

  // This offset is needed to write directly into the decompressed data vector.
  auto decompression_offset = size_t{0};
  for (auto block_index = size_t{0}; block_index < num_blocks; ++block_index) {
    _decompress_block(block_index, decompressed_data, decompression_offset);
    decompression_offset += _block_size;
  }
  return decompressed_data;
}

template <>
std::vector<pmr_string> LZ4Segment<pmr_string>::decompress() const {
  /**
   * If the input segment only contained empty strings, the original size is 0. The segment can't be decompressed, and
   * instead we can just return as many empty strings as the input contained.
   */
  if (_lz4_blocks.empty()) {
    return std::vector<pmr_string>(size());
  }

  const auto decompressed_size = (_lz4_blocks.size() - 1) * _block_size + _last_block_size;
  auto decompressed_data = std::vector<char>(decompressed_size);
  using DecompressedDataDifferenceType =
      typename std::iterator_traits<decltype(decompressed_data)::iterator>::difference_type;

  const auto num_blocks = _lz4_blocks.size();

  for (auto block_index = size_t{0}; block_index < num_blocks; ++block_index) {
    // This offset is needed to write directly into the decompressed data vector.
    const auto decompression_offset = block_index * _block_size;
    _decompress_block_to_bytes(block_index, decompressed_data, decompression_offset);
  }

  /**
   * Decode the previously encoded string data. These strings are all appended and separated along the stored offsets.
   * Each offset corresponds to a single string. The stored offset itself is the character offset of the first character
   * of the string. The end offset is the first character behind the string that is NOT part of the string (i.e., an
   * exclusive offset). It is usually the next offset in the vector. In the case of the last offset the end offset is
   * indicated by the end of the data vector.
   * The offsets are stored in a compressed vector and accessed via the vector decompression interface.
   */
  auto offset_decompressor = _string_offsets->create_base_decompressor();
  auto decompressed_strings = std::vector<pmr_string>();
  for (auto offset_index = size_t{0u}; offset_index < offset_decompressor->size(); ++offset_index) {
    auto start_char_offset = offset_decompressor->get(offset_index);
    auto end_char_offset = size_t{0};
    if (offset_index + 1 == offset_decompressor->size()) {
      end_char_offset = decompressed_size;
    } else {
      end_char_offset = offset_decompressor->get(offset_index + 1);
    }

    const auto start_offset_it =
        decompressed_data.cbegin() + static_cast<DecompressedDataDifferenceType>(start_char_offset);
    const auto end_offset_it =
        decompressed_data.cbegin() + static_cast<DecompressedDataDifferenceType>(end_char_offset);
    decompressed_strings.emplace_back(start_offset_it, end_offset_it);
  }

  return decompressed_strings;
}

template <typename T>
void LZ4Segment<T>::_decompress_block(const size_t block_index, std::vector<T>& decompressed_data,
                                      const size_t write_offset) const {
  const auto decompressed_block_size = block_index + 1 != _lz4_blocks.size() ? _block_size : _last_block_size;
  const auto& compressed_block = _lz4_blocks[block_index];
  const auto compressed_block_size = compressed_block.size();

  auto decompressed_result = int{0};
  if (_dictionary.empty()) {
    /**
     * If the dictionary is empty, we either have only a single block or had not enough data for a dictionary.
     * When decoding without a dictionary LZ4 needs a stream decode pointer (which would be used to decode the
     * following blocks).
     * A new decoder needs to be created for every block (in the case of multiple blocks being compressed without a
     * dictionary) since the blocks were compressed independently.
     * This decoder needs to be reset via LZ4_setStreamDecode since LZ4 reuses the previous state instead.
     */
    auto lz4_stream_decoder = std::make_unique<LZ4_streamDecode_t>();
    const auto reset_decoder_status = LZ4_setStreamDecode(lz4_stream_decoder.get(), nullptr, 0);
    Assert(reset_decoder_status == 1, "LZ4 decompression failed to reset stream decoder.");

    decompressed_result = LZ4_decompress_safe_continue(lz4_stream_decoder.get(), compressed_block.data(),
                                                       reinterpret_cast<char*>(decompressed_data.data()) + write_offset,
                                                       static_cast<int>(compressed_block_size),
                                                       static_cast<int>(decompressed_block_size));
  } else {
    decompressed_result = LZ4_decompress_safe_usingDict(
        compressed_block.data(), reinterpret_cast<char*>(decompressed_data.data()) + write_offset,
        static_cast<int>(compressed_block_size), static_cast<int>(decompressed_block_size), _dictionary.data(),
        static_cast<int>(_dictionary.size()));
  }

  Assert(decompressed_result > 0, "LZ4 stream decompression failed");
  DebugAssert(static_cast<size_t>(decompressed_result) == decompressed_block_size,
              "Decompressed LZ4 block has different size than the initial source data.");
}

template <typename T>
void LZ4Segment<T>::_decompress_block_to_bytes(const size_t block_index, std::vector<char>& decompressed_data) const {
  // Assure that the decompressed data fits into the vector.
  if (decompressed_data.size() != _block_size) {
    decompressed_data.resize(_block_size);
  }

  // We use the string method since we handle a char-vector (even though the data is no necessarily string data).
  _decompress_block_to_bytes(block_index, decompressed_data, 0u);

  /**
    * In the case of the last block, the decompressed data is possibly smaller than _block_size (its size equals
    * _last_block_size). However, when decompressing that block into a buffer of the size _last_block_size, the
    * LZ4 decompression fails. Therefore, the block is decompressed into a buffer of size _block_size and resized to
    * the smaller _last_block_size afterwards.
    */
  if (block_index + 1 == _lz4_blocks.size()) {
    decompressed_data.resize(_last_block_size);
  }
}

template <typename T>
void LZ4Segment<T>::_decompress_block_to_bytes(const size_t block_index, std::vector<char>& decompressed_data,
                                               const size_t write_offset) const {
  const auto decompressed_block_size = block_index + 1 != _lz4_blocks.size() ? _block_size : _last_block_size;
  const auto& compressed_block = _lz4_blocks.at(block_index);
  const auto compressed_block_size = compressed_block.size();

  auto decompressed_result = int{0};
  if (_dictionary.empty()) {
    /**
     * If the dictionary is empty, we either have only a single block or had not enough data for a dictionary.
     * When decoding without a dictionary LZ4 needs a stream decode pointer (which would be used to decode the
     * following blocks).
     * A new decoder needs to be created for every block (in the case of multiple blocks being compressed without a
     * dictionary) since the blocks were compressed independently.
     * This decoder needs to be reset via LZ4_setStreamDecode since LZ4 reuses the previous state instead.
     */
    auto lz4_stream_decoder = std::make_unique<LZ4_streamDecode_t>();
    const auto reset_decoder_status = LZ4_setStreamDecode(lz4_stream_decoder.get(), nullptr, 0);
    Assert(reset_decoder_status == 1, "LZ4 decompression failed to reset stream decoder.");

    decompressed_result = LZ4_decompress_safe_continue(
        lz4_stream_decoder.get(), compressed_block.data(), decompressed_data.data() + write_offset,
        static_cast<int>(compressed_block_size), static_cast<int>(decompressed_block_size));
  } else {
    decompressed_result = LZ4_decompress_safe_usingDict(
        compressed_block.data(), decompressed_data.data() + write_offset, static_cast<int>(compressed_block_size),
        static_cast<int>(decompressed_block_size), _dictionary.data(), static_cast<int>(_dictionary.size()));
  }

  Assert(decompressed_result > 0, "LZ4 stream decompression failed");
  DebugAssert(static_cast<size_t>(decompressed_result) == decompressed_block_size,
              "Decompressed LZ4 block has different size than the initial source data.");
}

template <typename T>
std::pair<T, size_t> LZ4Segment<T>::decompress(const ChunkOffset& chunk_offset,
                                               const std::optional<size_t> cached_block_index,
                                               std::vector<char>& cached_block) const {
  const auto memory_offset = chunk_offset * sizeof(T);
  const auto block_index = memory_offset / _block_size;

  /**
   * If the previously decompressed block was a different block than the one accessed now, overwrite it with the now
   * decompressed block.
   */
  if (!cached_block_index || block_index != *cached_block_index) {
    _decompress_block_to_bytes(block_index, cached_block);
  }

  const auto value_offset = (memory_offset % _block_size) / sizeof(T);
  const T value = *(reinterpret_cast<T*>(cached_block.data()) + value_offset);
  return std::pair{value, block_index};
}

template <>
std::pair<pmr_string, size_t> LZ4Segment<pmr_string>::decompress(const ChunkOffset& chunk_offset,
                                                                 const std::optional<size_t> cached_block_index,
                                                                 std::vector<char>& cached_block) const {
  using CachedBlockDifferenceType =
      typename std::iterator_traits<std::decay_t<decltype(cached_block)>::iterator>::difference_type;

  /**
   * If the input segment only contained empty strings, the original size is 0. The segment can't be decompressed, and
   * instead we can just return as many empty strings as the input contained.
   */
  if (_lz4_blocks.empty()) {
    return std::pair{pmr_string{""}, size_t{0}};
  }

  /**
   * Calculate character begin and end offsets. This range may span more than one block. If this is the case, multiple
   * blocks need to be decompressed.
   * The offsets are stored in a compressed vector and accessed via the vector decompression interface.
   */
  auto offset_decompressor = _string_offsets->create_base_decompressor();
  auto start_offset = offset_decompressor->get(chunk_offset);
  auto end_offset = size_t{0};
  if (chunk_offset + 1 == offset_decompressor->size()) {
    end_offset = (_lz4_blocks.size() - 1) * _block_size + _last_block_size;
  } else {
    end_offset = offset_decompressor->get(chunk_offset + 1);
  }

  /**
   * Find the block range in which the string is. If it is only in a single block, then the decompression is simple.
   * Otherwise multiple blocks need to be decompressed.
   */
  const auto start_block = start_offset / _block_size;
  const auto end_block = end_offset / _block_size;

  // Only one block needs to be decompressed.
  if (start_block == end_block) {
    /**
     * If the previously decompressed block was a different block than the one accessed now, overwrite it with the now
     * decompressed block.
     */
    if (!cached_block_index || start_block != *cached_block_index) {
      _decompress_block_to_bytes(start_block, cached_block);
    }

    // Extract the string from the block via the offsets.
    const auto block_start_offset = start_offset % _block_size;
    const auto block_end_offset = end_offset % _block_size;
    const auto start_offset_it = cached_block.cbegin() + static_cast<CachedBlockDifferenceType>(block_start_offset);
    const auto end_offset_it = cached_block.cbegin() + static_cast<CachedBlockDifferenceType>(block_end_offset);

    return std::pair{pmr_string{start_offset_it, end_offset_it}, start_block};
  }

  /**
   * Multiple blocks need to be decompressed. Iterate over all relevant blocks and append the result to this string
   * stream.
   */
  auto result_stringstream = std::stringstream{};

  // These are the character offsets that need to be read in every block.
  auto block_start_offset = start_offset % _block_size;
  auto block_end_offset = _block_size;

  /**
   * This is true if there is a block cached and it is one of the blocks that has to be accessed to decompress the
   * current element.
   * If it is true there are two cases:
   * 1) The first block that has to be accesses is cached. This is trivial and afterwards the data can be overwritten.
   * 2) The cached block is not the first but a later block. In that case, the cached block is copied. The original
   * buffer is overwritten when decompressing the other blocks. When the cached block needs to be accessed, the copy
   * is used.
   */
  const auto use_caching =
      cached_block_index && *cached_block_index >= start_block && *cached_block_index <= end_offset;

  /**
   * If the cached block is not the first block, keep a copy so that the blocks can still be decompressed into the
   * passed char array and the last decompressed block will be cached afterwards.
   */
  auto cached_block_copy = std::vector<char>{};
  if (use_caching && *cached_block_index != start_block) {
    cached_block_copy = std::vector<char>{cached_block};
  }

  /**
   * Store the index of the last decompressed block. The blocks are decompressed into the cache buffer. If the cached
   * block is the last block the string, it is copied and used. As a result, the cache contains the last decompressed
   * block (i.e., the block before the cached block).
   * In that case, this index equals end_block - 1. Otherwise, it will equal end_block.
   */
  auto new_cached_block_index = size_t{0};

  for (auto block_index = start_block; block_index <= end_block; ++block_index) {
    // Only decompress the current block if it's not cached.
    if (!use_caching || block_index != *cached_block_index) {
      _decompress_block_to_bytes(block_index, cached_block);
      new_cached_block_index = block_index;
    }

    // Set the offset for the end of the string.
    if (block_index == end_block) {
      block_end_offset = end_offset % _block_size;
    }

    /**
     * Extract the string from the current block via the offsets and append it to the result string stream.
     * If the cached block is not the start block, the data is retrieved from the copy.
     */
    auto partial_result = pmr_string{};
    if (use_caching && block_index == *cached_block_index && block_index != start_block) {
      const auto start_offset_it =
          cached_block_copy.cbegin() + static_cast<CachedBlockDifferenceType>(block_start_offset);
      const auto end_offset_it = cached_block_copy.cbegin() + static_cast<CachedBlockDifferenceType>(block_end_offset);
      partial_result = pmr_string{start_offset_it, end_offset_it};
    } else {
      const auto start_offset_it = cached_block.cbegin() + static_cast<CachedBlockDifferenceType>(block_start_offset);
      const auto end_offset_it = cached_block.cbegin() + static_cast<CachedBlockDifferenceType>(block_end_offset);
      partial_result = pmr_string{start_offset_it, end_offset_it};
    }
    result_stringstream << partial_result;

    // After the first iteration, this is set to 0 since only the first block's start offset can't be equal to zero.
    block_start_offset = 0;
  }
  return std::pair{pmr_string{result_stringstream.str()}, new_cached_block_index};
}

template <typename T>
T LZ4Segment<T>::decompress(const ChunkOffset& chunk_offset) const {
  auto decompressed_block = std::vector<char>(_block_size, char{});
  return decompress(chunk_offset, std::nullopt, decompressed_block).first;
}

template <typename T>
std::shared_ptr<AbstractSegment> LZ4Segment<T>::copy_using_memory_resource(MemoryResource& memory_resource) const {
  auto new_lz4_blocks = pmr_vector<pmr_vector<char>>{&memory_resource};
  for (const auto& block : _lz4_blocks) {
    auto block_copy = pmr_vector<char>{block, &memory_resource};
    new_lz4_blocks.emplace_back(std::move(block_copy));
  }

  auto new_null_values =
      _null_values ? std::optional<pmr_vector<bool>>{pmr_vector<bool>{*_null_values, &memory_resource}} : std::nullopt;
  auto new_dictionary = pmr_vector<char>{_dictionary, &memory_resource};

  auto copy = std::shared_ptr<LZ4Segment<T>>{};

  if (_string_offsets) {
    auto new_string_offsets = _string_offsets ? _string_offsets->copy_using_memory_resource(memory_resource) : nullptr;
    copy = std::make_shared<LZ4Segment<T>>(std::move(new_lz4_blocks), std::move(new_null_values),
                                           std::move(new_dictionary), std::move(new_string_offsets), _block_size,
                                           _last_block_size, _compressed_size, _num_elements);
  } else {
    copy = std::make_shared<LZ4Segment<T>>(std::move(new_lz4_blocks), std::move(new_null_values),
                                           std::move(new_dictionary), _block_size, _last_block_size, _compressed_size,
                                           _num_elements);
  }

  copy->access_counter = access_counter;

  return copy;
}

template <typename T>
size_t LZ4Segment<T>::memory_usage(const MemoryUsageCalculationMode /*mode*/) const {
  // MemoryUsageCalculationMode can be ignored since all relevant information can be either obtained directly (e.g.,
  // size of NULL values vector) or the actual size is already stored (e.g., data_size()).

  // The null value vector is only stored if there is at least 1 null value in the segment.
  auto null_value_vector_size = size_t{0u};
  if (_null_values) {
    null_value_vector_size = _null_values->capacity() / CHAR_BIT;
  }

  // The overhead of storing each block in a separate vector.
  auto block_vector_size = _lz4_blocks.size() * sizeof(pmr_vector<char>);

  /**
   * _string_offsets is used only for string segments and is a nullptr if the string segment does not contain any data
   * (i.e., no rows or only rows with empty strings).
   */
  auto offset_size = size_t{0};
  if (_string_offsets) {
    offset_size = _string_offsets->data_size();
  }
  return sizeof(*this) + _compressed_size + null_value_vector_size + offset_size + _dictionary.size() +
         block_vector_size;
}

template <typename T>
EncodingType LZ4Segment<T>::encoding_type() const {
  return EncodingType::LZ4;
}

template <typename T>
std::optional<CompressedVectorType> LZ4Segment<T>::compressed_vector_type() const {
  return std::nullopt;
}

// Right now, vector compression is fixed to BitPacking. This method nonetheless checks for the actual vector
// compression type. So if the vector compression becomes configurable, this method does not need to be touched.
template <>
std::optional<CompressedVectorType> LZ4Segment<pmr_string>::compressed_vector_type() const {
  auto type = std::optional<CompressedVectorType>{};
  if (_string_offsets) {
    return _string_offsets->type();
  }
  return type;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(LZ4Segment);

}  // namespace hyrise
