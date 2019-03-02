#include "lz4_segment.hpp"

#include <lz4.h>

#include <sstream>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, pmr_vector<bool>&& null_values,
                          pmr_vector<char>&& dictionary, const size_t block_size, const size_t last_block_size,
                          const size_t compressed_size)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _lz4_blocks{std::move(lz4_blocks)},
      _null_values{std::move(null_values)},
      _dictionary{std::move(dictionary)},
      _string_offsets{std::nullopt},
      _block_size{block_size},
      _last_block_size{last_block_size},
      _compressed_size{compressed_size} {}

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<pmr_vector<char>>&& lz4_blocks, pmr_vector<bool>&& null_values,
                          pmr_vector<char>&& dictionary, pmr_vector<size_t>&& string_offsets, const size_t block_size,
                          const size_t last_block_size, const size_t compressed_size)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _lz4_blocks{std::move(lz4_blocks)},
      _null_values{std::move(null_values)},
      _dictionary{std::move(dictionary)},
      _string_offsets{std::move(string_offsets)},
      _block_size{block_size},
      _last_block_size{last_block_size},
      _compressed_size{compressed_size} {}

template <typename T>
const AllTypeVariant LZ4Segment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value.has_value()) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
const std::optional<T> LZ4Segment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  const auto is_null = _null_values[chunk_offset];
  if (is_null) {
    return std::nullopt;
  }

  return decompress(chunk_offset);
}

template <typename T>
const pmr_vector<bool>& LZ4Segment<T>::null_values() const {
  return _null_values;
}

template <typename T>
const std::optional<const pmr_vector<size_t>> LZ4Segment<T>::string_offsets() const {
  return _string_offsets;
}

template <typename T>
const pmr_vector<char>& LZ4Segment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
size_t LZ4Segment<T>::size() const {
  return _null_values.size();
}

template <typename T>
std::vector<T> LZ4Segment<T>::decompress() const {
  auto decompressed_data = std::vector<T>(size());

  const auto num_blocks = _lz4_blocks.size();

  // We wrap the stream decoder in a unique pointer since LZ4 expects a pointer to the decoder as argument.
  LZ4_streamDecode_t lz4_stream_decoder;
  auto lz4_stream_decoder_ptr = std::make_unique<LZ4_streamDecode_t>(lz4_stream_decoder);

  for (size_t block_index = 0u; block_index < num_blocks; ++block_index) {
    // This offset is needed to write directly into the decompressed data vector.
    const auto decompression_offset = block_index * _block_size / sizeof(T);
    _decompress_block(lz4_stream_decoder_ptr, block_index, decompressed_data, decompression_offset);
  }
  return decompressed_data;
}

template <>
std::vector<pmr_string> LZ4Segment<pmr_string>::decompress() const {
  /**
    * If the input segment only contained empty strings the original size is 0. That can't be decompressed and instead
    * we can just return as many empty strings as the input contained.
    */
  if (!_lz4_blocks.size()) {
    return std::vector<pmr_string>(_null_values.size());
  }

  const auto decompressed_size = (_lz4_blocks.size() - 1) * _block_size + _last_block_size;
  auto decompressed_data = std::vector<char>(decompressed_size);

  const auto num_blocks = _lz4_blocks.size();

  // We wrap the stream decoder in a unique pointer since LZ4 expects a pointer to the decoder as argument.
  LZ4_streamDecode_t lz4_stream_decoder;
  auto lz4_stream_decoder_ptr = std::make_unique<LZ4_streamDecode_t>(lz4_stream_decoder);

  for (size_t block_index = 0u; block_index < num_blocks; ++block_index) {
    // This offset is needed to write directly into the decompressed data vector.
    const auto decompression_offset = block_index * _block_size;
    _decompress_string_block(lz4_stream_decoder_ptr, block_index, decompressed_data, decompression_offset);
  }

  /**
   * Decode the previously encoded string data. These strings are all appended and separated along the stored offsets.
   * Each offset corresponds to a single string. The stored offset itself is the character offset of the first character
   * of the string. The end offset is the first character behind the string that is NOT part of the string (i.e., an
   * exclusive offset). It is usually the next offset in the vector. In the case of the last offset the end offset is
   * indicated by the end of the data vector.
   */
  auto decompressed_strings = std::vector<pmr_string>();
  for (auto it = _string_offsets->cbegin(); it != _string_offsets->cend(); ++it) {
    auto start_char_offset = *it;
    size_t end_char_offset;
    if (it + 1 == _string_offsets->cend()) {
      end_char_offset = decompressed_size;
    } else {
      end_char_offset = *(it + 1);
    }

    const auto start_offset_it = decompressed_data.cbegin() + start_char_offset;
    const auto end_offset_it = decompressed_data.cbegin() + end_char_offset;
    decompressed_strings.emplace_back(start_offset_it, end_offset_it);
  }

  return decompressed_strings;
}

template <typename T>
void LZ4Segment<T>::_decompress_block(const size_t block_index, std::vector<T>& decompressed_data,
                                      const size_t write_offset) const {
  // We wrap the stream decoder in a unique pointer since LZ4 expects a pointer to the decoder as argument.
  LZ4_streamDecode_t lz4_stream_decoder;
  auto lz4_stream_decoder_ptr = std::make_unique<LZ4_streamDecode_t>(lz4_stream_decoder);
  _decompress_block(lz4_stream_decoder_ptr, block_index, decompressed_data, write_offset);
}

template <typename T>
void LZ4Segment<T>::_decompress_block(std::unique_ptr<LZ4_streamDecode_t>& lz4_stream_decoder_ptr,
                                      const size_t block_index, std::vector<T>& decompressed_data,
                                      const size_t write_offset) const {
  auto decompressed_block_size = _block_size;
  if (block_index + 1 == _lz4_blocks.size()) {
    decompressed_block_size = _last_block_size;
  }
  auto& compressed_block = _lz4_blocks[block_index];
  const auto compressed_block_size = compressed_block.size();

  if (!_dictionary.empty()) {
    int success =
        LZ4_setStreamDecode(lz4_stream_decoder_ptr.get(), _dictionary.data(), static_cast<int>(_dictionary.size()));
    Assert(success == 1, "Setting the dictionary in LZ4 decompression failed.");
  }

  const int decompressed_result =
      LZ4_decompress_safe_continue(lz4_stream_decoder_ptr.get(), compressed_block.data(),
                                   reinterpret_cast<char*>(decompressed_data.data()) + write_offset,
                                   static_cast<int>(compressed_block_size), static_cast<int>(decompressed_block_size));

  Assert(decompressed_result > 0, "LZ4 stream decompression failed");
  DebugAssert(static_cast<size_t>(decompressed_result) == decompressed_block_size,
              "Decompressed LZ4 block has different size than the initial source data.")
}

template <typename T>
void LZ4Segment<T>::_decompress_string_block(const size_t block_index, std::vector<char>& decompressed_data) const {
  // We wrap the stream decoder in a unique pointer since LZ4 expects a pointer to the decoder as argument.
  LZ4_streamDecode_t lz4_stream_decoder;
  auto lz4_stream_decoder_ptr = std::make_unique<LZ4_streamDecode_t>(lz4_stream_decoder);
  _decompress_string_block(lz4_stream_decoder_ptr, block_index, decompressed_data, 0u);
}

template <typename T>
void LZ4Segment<T>::_decompress_string_block(std::unique_ptr<LZ4_streamDecode_t>& lz4_stream_decoder_ptr,
                                             const size_t block_index, std::vector<char>& decompressed_data,
                                             const size_t write_offset) const {
  auto decompressed_block_size = _block_size;
  if (block_index + 1 == _lz4_blocks.size()) {
    decompressed_block_size = _last_block_size;
  }
  auto& compressed_block = _lz4_blocks[block_index];
  const auto compressed_block_size = compressed_block.size();

  if (!_dictionary.empty()) {
    int success =
        LZ4_setStreamDecode(lz4_stream_decoder_ptr.get(), _dictionary.data(), static_cast<int>(_dictionary.size()));
    DebugAssert(success == 1, "Setting the dictionary in LZ4 decompression failed.");
  }

  const int decompressed_result = LZ4_decompress_safe_continue(
      lz4_stream_decoder_ptr.get(), compressed_block.data(), decompressed_data.data() + write_offset,
      static_cast<int>(compressed_block_size), static_cast<int>(decompressed_block_size));

  Assert(decompressed_result > 0, "LZ4 stream decompression failed");
  DebugAssert(static_cast<size_t>(decompressed_result) == decompressed_block_size,
              "Decompressed LZ4 block has different size than the initial source data.")
}

template <typename T>
T LZ4Segment<T>::decompress(const ChunkOffset& chunk_offset) const {
  const auto memory_offset = chunk_offset * sizeof(T);
  const auto block_index = memory_offset / _block_size;
  auto decompressed_block_size = _block_size;
  if (block_index + 1 == _lz4_blocks.size()) {
    decompressed_block_size = _last_block_size;
  }
  auto decompressed_block = std::vector<T>(decompressed_block_size / sizeof(T));

  _decompress_block(block_index, decompressed_block, 0u);

  const auto value_offset = (memory_offset - (block_index * _block_size)) / sizeof(T);
  return decompressed_block[value_offset];
}

template <>
pmr_string LZ4Segment<pmr_string>::decompress(const ChunkOffset& chunk_offset) const {
  /**
    * If the input segment only contained empty strings the original size is 0. That can't be decompressed and instead
    * we can just return as many empty strings as the input contained.
    */
  if (!_lz4_blocks.size()) {
    return pmr_string{""};
  }

  /**
   * Calculate character being and end offsets. This range may span more than block. If this is the case multiple
   * blocks need to be decompressed.
   */
  const auto start_offset = _string_offsets->at(chunk_offset);
  size_t end_offset;
  if (chunk_offset + 1 == _string_offsets->size()) {
    end_offset = (_lz4_blocks.size() - 1) * _block_size + _last_block_size;
  } else {
    end_offset = _string_offsets->at(chunk_offset + 1);
  }

  const auto start_block = start_offset / _block_size;
  const auto end_block = end_offset / _block_size;

  // Only one block needs to be decompressed.
  if (start_block == end_block) {
    auto decompressed_block_size = _block_size;
    if (start_block + 1 == _lz4_blocks.size()) {
      decompressed_block_size = _last_block_size;
    }
    auto decompressed_block = std::vector<char>(decompressed_block_size);
    _decompress_string_block(start_block, decompressed_block);
    const auto block_start_offset = start_offset % _block_size;
    const auto block_end_offset = end_offset % _block_size;
    const auto start_offset_it = decompressed_block.cbegin() + block_start_offset;
    const auto end_offset_it = decompressed_block.cbegin() + block_end_offset;

    return pmr_string{start_offset_it, end_offset_it};
  } else {
    std::cout << "Decompressing multi block string in [" << start_offset << ", " << end_offset << ")" << std::endl;
    std::cout << "Start block " << start_block << " and end block " << end_block << std::endl;

    LZ4_streamDecode_t lz4_stream_decoder;
    auto lz4_stream_decoder_ptr = std::make_unique<LZ4_streamDecode_t>(lz4_stream_decoder);
    const auto reach_last_block = end_block + 1 == _lz4_blocks.size();

    std::stringstream result_string;
    size_t block_start_offset = start_offset % _block_size;
    size_t block_end_offset = _block_size;

    // Iterate over all blocks in the range including the last (end) block
    for (size_t block_index = 0u; block_index <= end_block - start_block; ++block_index) {
      const auto current_block = start_block + block_index;
      std::cout << "Decompressing block " << current_block << std::endl;
      const auto block_size = current_block == end_block && reach_last_block ? _last_block_size : _block_size;
      if (current_block == end_block) {
        block_end_offset = end_offset % _block_size;
      }
      std::cout << "Reading in block from " << block_start_offset << " to " << block_end_offset << std::endl;

      auto decompressed_block = std::vector<char>(block_size);
      _decompress_string_block(start_block, decompressed_block);

      const auto start_offset_it = decompressed_block.cbegin() + block_start_offset;
      const auto end_offset_it = decompressed_block.cbegin() + block_end_offset;
      auto partial_result = pmr_string{start_offset_it, end_offset_it};
      std::cout << "Adding partial result " << partial_result << std::endl;
      result_string << partial_result;

      block_start_offset = 0u;
    }
    return pmr_string{result_string.str()};
  }
}

template <typename T>
std::shared_ptr<BaseSegment> LZ4Segment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_lz4_blocks = pmr_vector<pmr_vector<char>>{alloc};
  for (const auto& block : _lz4_blocks) {
    new_lz4_blocks.emplace_back(pmr_vector<char>{block, alloc});
  }
  auto new_null_values = pmr_vector<bool>{_null_values, alloc};
  auto new_dictionary = pmr_vector<char>{_dictionary, alloc};

  if (_string_offsets.has_value()) {
    auto new_string_offsets = pmr_vector<size_t>(*_string_offsets, alloc);
    return std::allocate_shared<LZ4Segment>(alloc, std::move(new_lz4_blocks), std::move(new_null_values),
                                            std::move(new_dictionary), std::move(new_string_offsets), _block_size,
                                            _last_block_size, _compressed_size);
  } else {
    return std::allocate_shared<LZ4Segment>(alloc, std::move(new_lz4_blocks), std::move(new_null_values),
                                            std::move(new_dictionary), _block_size, _last_block_size, _compressed_size);
  }
}

template <typename T>
size_t LZ4Segment<T>::estimate_memory_usage() const {
  // TODO: add vector of vector size (i.e., references)
  auto bool_size = _null_values.size() * sizeof(bool);
  // _offsets is used only for strings
  auto offset_size = (_string_offsets.has_value() ? _string_offsets->size() * sizeof(size_t) : 0u);
  return sizeof(*this) + _compressed_size + bool_size + offset_size + _dictionary.size();
}

template <typename T>
EncodingType LZ4Segment<T>::encoding_type() const {
  return EncodingType::LZ4;
}

template <typename T>
std::optional<CompressedVectorType> LZ4Segment<T>::compressed_vector_type() const {
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(LZ4Segment);

}  // namespace opossum
