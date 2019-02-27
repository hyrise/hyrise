#include "lz4_segment.hpp"

#include <lz4.h>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<char>&& compressed_data,
                          pmr_vector<bool>&& null_values,
                          const size_t decompressed_size,
                          const std::shared_ptr<const pmr_vector<char>>& dictionary)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _compressed_data{std::move(compressed_data)},
      _null_values{std::move(null_values)},
      _offsets{std::nullopt},
      _decompressed_size{decompressed_size},
      _dictionary{dictionary},
      _string_offsets{nullptr} {}

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<char>&& compressed_data,
                          pmr_vector<bool>&& null_values,
                          pmr_vector<size_t>&& offsets,
                          const size_t decompressed_size,
                          const std::shared_ptr<const pmr_vector<char>>& dictionary,
                          const std::shared_ptr<const pmr_vector<size_t>>& string_offsets)
    : BaseEncodedSegment{data_type_from_type<std::string>()},
      _compressed_data{std::move(compressed_data)},
      _null_values{std::move(null_values)},
      _offsets{std::move(offsets)},
      _decompressed_size{decompressed_size},
      _dictionary{dictionary},
      _string_offsets{string_offsets}  {}

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
const std::optional<const pmr_vector<size_t>> LZ4Segment<T>::offsets() const {
  return _offsets;
}

template <typename T>
std::shared_ptr<const pmr_vector<char>> LZ4Segment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
size_t LZ4Segment<T>::size() const {
  return _null_values.size();
}

template <typename T>
std::shared_ptr<std::vector<T>> LZ4Segment<T>::decompress() const {
  auto decompressed_data = std::make_shared<std::vector<T>>(_decompressed_size / sizeof(T));

  const int block_size = 4096;
  const int num_blocks = static_cast<int>(_offsets->size());

  LZ4_streamDecode_t stream_decode;
  const auto stream_decode_ptr = std::make_unique<LZ4_streamDecode_t>(stream_decode);

  for (int block_count = 0; block_count < num_blocks; ++block_count) {
    const int decompressed_block_size = block_count + 1 == num_blocks ? _decompressed_size - (block_size * block_count) : block_size;
    const int compressed_block_size = block_count == 0 ? _offsets->at(0) : _offsets->at(block_count) - _offsets->at(block_count - 1);
    std::vector<char> decompressed_block(static_cast<size_t>(decompressed_block_size));
    size_t offset = block_count == 0 ? 0 : _offsets->at(block_count - 1);

    if (_dictionary != nullptr) {
      int success = LZ4_setStreamDecode(stream_decode_ptr.get(), _dictionary->data(), static_cast<int>(_dictionary->size()));
      DebugAssert(success == 1, "Error while setting dictionary for LZ4 decompression");
    }
    const int decompressed_len = LZ4_decompress_safe_continue(
                                  stream_decode_ptr.get(),
                                  _compressed_data->data() + offset,
                                  decompressed_block.data(),
                                  compressed_block_size,
                                  decompressed_block_size);

    if (decompressed_len != decompressed_block_size) {
      Fail("LZ4 stream decompression failed");
    }

    auto data_ptr = reinterpret_cast<char*>(decompressed_data->data()) + (block_count * block_size);
    std::memcpy(data_ptr, decompressed_block.data(), static_cast<size_t>(decompressed_block_size));
  }

    return decompressed_data;
}

//std::vector<T> LZ4Segment<T>::decompress() const {
//  auto decompressed_data = std::vector<T>(_decompressed_size / sizeof(T));
//  auto compressed_size = static_cast<int>(_compressed_data.size());
//  const int decompressed_result =
//      LZ4_decompress_safe(_compressed_data.data(), reinterpret_cast<char*>(decompressed_data.data()), compressed_size,
//                          static_cast<int>(_decompressed_size));
//  Assert(decompressed_result > 0, "LZ4 decompression failed");
//
//  return decompressed_data;
//}

//template <>
//std::vector<pmr_string> LZ4Segment<pmr_string>::decompress() const {
//  /**
//   * If the input segment only contained empty strings the original size is 0. That can't be decompressed and instead
//   * we can just return as many empty strings as the input contained.
//   */
//  if (!_decompressed_size) {
//    return std::vector<pmr_string>(_null_values.size());
//  }
//
//  auto decompressed_data = std::vector<char>(_decompressed_size);
//  auto compressed_size = static_cast<int>(_compressed_data.size());
//  const int decompressed_result = LZ4_decompress_safe(_compressed_data.data(), decompressed_data.data(),
//                                                      compressed_size, static_cast<int>(_decompressed_size));
//  Assert(decompressed_result > 0, "LZ4 decompression failed");
//
//  /**
//   * Decode the previously encoded string data. These strings are all appended and separated along the stored offsets.
//   * Each offset corresponds to a single string. The stored offset itself is the character offset of the first character
//   * of the string. The end offset is the first character behind the string that is NOT part of the string (i.e., an
//   * exclusive offset). It is usually the next offset in the vector. In the case of the last offset the end offset is
//   * indicated by the end of the data vector.
//   */
//  auto decompressed_strings = std::vector<pmr_string>();
//  for (auto it = _offsets->cbegin(); it != _offsets->cend(); ++it) {
//    auto start_char_offset = *it;
//    size_t end_char_offset;
//    if (it + 1 == _offsets->cend()) {
//      end_char_offset = _decompressed_size;
//    } else {
//      end_char_offset = *(it + 1);
//    }
//
//    const auto start_offset_it = decompressed_data.cbegin() + start_char_offset;
//    const auto end_offset_it = decompressed_data.cbegin() + end_char_offset;
//    decompressed_strings.emplace_back(start_offset_it, end_offset_it);
//  }
//
//  return decompressed_strings;
//}

template <typename T>
T LZ4Segment<T>::decompress(const ChunkOffset &chunk_offset) const {
  auto decompressed_data = std::vector<T>(_decompressed_size / sizeof(T));

  const int block_size = 4096;
  const int num_blocks = static_cast<int>(_offsets->size());

  LZ4_streamDecode_t stream_decode;
  const auto stream_decode_ptr = std::make_unique<LZ4_streamDecode_t>(stream_decode);

  const auto position = chunk_offset * sizeof(T);
  const int block_id = position / block_size;
  const int decompressed_block_size =
          block_id + 1 == num_blocks ? _decompressed_size - (block_size * block_id) : block_size;
  const int compressed_block_size =
          block_id == 0 ? _offsets->at(0) : _offsets->at(block_id) - _offsets->at(block_id - 1);
  std::vector<char> decompressed_block(static_cast<size_t>(decompressed_block_size));
  size_t offset = block_id == 0 ? 0 : _offsets->at(block_id - 1);

  if (_dictionary != nullptr) {
    int success = LZ4_setStreamDecode(stream_decode_ptr.get(), _dictionary->data(),
                                      static_cast<int>(_dictionary->size()));
    DebugAssert(success == 1, "Error while setting dictionary for LZ4 decompression");
  }
  const int decompressed_len = LZ4_decompress_safe_continue(
          stream_decode_ptr.get(),
          _compressed_data->data() + offset,
          decompressed_block.data(),
          compressed_block_size,
          decompressed_block_size);

  if (decompressed_len != decompressed_block_size) {
    Fail("LZ4 stream decompression failed");
  }

  auto value = reinterpret_cast<T*>(decompressed_block.data() + (position - (block_id * block_size)));

  return *value;
}

template <>
std::shared_ptr<std::vector<std::string>> LZ4Segment<std::string>::decompress() const {
  auto decompressed_data = std::vector<char>(_decompressed_size);

  const int block_size = 4096;
  const int num_blocks = static_cast<int>(_offsets->size());

  LZ4_streamDecode_t stream_decode;
  const auto stream_decode_ptr = std::make_unique<LZ4_streamDecode_t>(stream_decode);

  for (int block_count = 0; block_count < num_blocks; ++block_count) {
    const int decompressed_block_size = block_count + 1 == num_blocks ? _decompressed_size - (block_size * block_count) : block_size;
    const int compressed_block_size = block_count == 0 ? _offsets->at(0) : _offsets->at(block_count) - _offsets->at(block_count - 1);
    std::vector<char> decompressed_block(static_cast<size_t>(decompressed_block_size));
    size_t offset = block_count == 0 ? 0 : _offsets->at(block_count - 1);

    if (_dictionary != nullptr) {
      int success = LZ4_setStreamDecode(stream_decode_ptr.get(), _dictionary->data(), static_cast<int>(_dictionary->size()));
      DebugAssert(success == 1, "Error while setting dictionary for LZ4 decompression");
    }
    const int decompressed_len = LZ4_decompress_safe_continue(
      stream_decode_ptr.get(),
      _compressed_data->data() + offset,
      decompressed_block.data(),
      compressed_block_size,
      decompressed_block_size);

    if (decompressed_len != decompressed_block_size) {
      Fail("LZ4 stream decompression failed");
    }

    auto data_ptr = decompressed_data.data() + (block_count * block_size);
    std::memcpy(data_ptr, decompressed_block.data(), static_cast<size_t>(decompressed_block_size));
  }

  auto decompressed_strings = std::make_shared<std::vector<std::string>>();

  for (auto it = _string_offsets->cbegin(); it != _string_offsets->cend(); ++it) {
    auto begin = *it;
    size_t end;
    if (it + 1 == _string_offsets->cend()) {
      end = static_cast<size_t>(_decompressed_size);
    } else {
      end = *(it + 1);
    }

    const auto data_begin = decompressed_data.cbegin() + begin;
    const auto data_end = decompressed_data.cbegin() + end;
    decompressed_strings->emplace_back(data_begin, data_end);
  }

  return decompressed_strings;
}

template <>
std::string LZ4Segment<std::string>::decompress(const ChunkOffset &chunk_offset) const {
  const int block_size = 4096;
  const int num_blocks = static_cast<int>(_offsets->size());

  LZ4_streamDecode_t stream_decode;
  const auto stream_decode_ptr = std::make_unique<LZ4_streamDecode_t>(stream_decode);

  const auto position = _string_offsets->at(chunk_offset);
  const auto string_length = _string_offsets->at(chunk_offset + 1) - position;
  std::vector<int> block_ids{};
  for (size_t i = position; i < position + string_length; i += block_size) {
    block_ids.emplace_back(i / block_size);
  }

  auto decompressed_data = std::vector<char>(string_length);
  auto decompressed_data_ptr = decompressed_data.data();

  for (auto block_id : block_ids) {
    const int decompressed_block_size =
      block_id + 1 == num_blocks ? _decompressed_size - (block_size * block_id) : block_size;
    const int compressed_block_size =
      block_id == 0 ? _offsets->at(0) : _offsets->at(block_id) - _offsets->at(block_id - 1);
    std::vector<char> decompressed_block(static_cast<size_t>(decompressed_block_size));
    size_t offset = block_id == 0 ? 0 : _offsets->at(block_id - 1);

    if (_dictionary != nullptr) {
      int success = LZ4_setStreamDecode(stream_decode_ptr.get(), _dictionary->data(),
                                        static_cast<int>(_dictionary->size()));
      DebugAssert(success == 1, "Error while setting dictionary for LZ4 decompression");
    }
    const int decompressed_len = LZ4_decompress_safe_continue(
      stream_decode_ptr.get(),
      _compressed_data->data() + offset,
      decompressed_block.data(),
      compressed_block_size,
      decompressed_block_size);

    if (decompressed_len != decompressed_block_size) {
      Fail("LZ4 stream decompression failed");
    }

    std::memcpy(decompressed_data_ptr, decompressed_block.data(), static_cast<size_t>(decompressed_block_size));
    decompressed_data_ptr += block_size; // move ptr forth by one block size for the next block's decompressed data
  }

  return std::string(decompressed_data.data(), string_length);
}

template <typename T>
std::shared_ptr<BaseSegment> LZ4Segment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_compressed_data = pmr_vector<char>{_compressed_data, alloc};
  auto new_null_values = pmr_vector<bool>{_null_values, alloc};
  // TODO: add new dictionary and string offsets

  if (_offsets.has_value()) {
    auto new_offsets = pmr_vector<size_t>(*_offsets, alloc);
    return std::allocate_shared<LZ4Segment>(alloc, std::move(new_compressed_data), std::move(new_null_values),
                                            std::move(new_offsets), _decompressed_size);
  } else {
    return std::allocate_shared<LZ4Segment>(alloc, std::move(new_compressed_data), std::move(new_null_values),
                                            _decompressed_size);
  }
}

template <typename T>
size_t LZ4Segment<T>::estimate_memory_usage() const {
  // TODO: add new member variables
  auto bool_size = _null_values.size() * sizeof(bool);
  // _offsets is used only for strings
  auto offset_size = (_offsets.has_value() ? _offsets->size() * sizeof(size_t) : 0u);
  return sizeof(*this) + _compressed_data.size() + bool_size + offset_size;
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
