#include "lz4_segment.hpp"

#include "lib/lz4.h"
#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
LZ4Segment<T>::LZ4Segment(const std::shared_ptr<const pmr_vector<char>>& compressed_data,
                          const std::shared_ptr<const pmr_vector<bool>>& null_values,
                          const std::shared_ptr<const pmr_vector<size_t>>& offsets,
                          const int compressed_size,
                          const int decompressed_size,
                          const size_t num_elements,
                          const std::shared_ptr<const pmr_vector<char>>& dictionary)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _compressed_data{compressed_data},
      _null_values{null_values},
      _offsets{offsets},
      _compressed_size{compressed_size},
      _decompressed_size{decompressed_size},
      _num_elements{num_elements},
      _dictionary{dictionary}  {}

template <typename T>
std::shared_ptr<const pmr_vector<char>> LZ4Segment<T>::compressed_data() const {
  return _compressed_data;
}

template <typename T>
std::shared_ptr<const pmr_vector<bool>> LZ4Segment<T>::null_values() const {
  return _null_values;
}

template <typename T>
std::shared_ptr<const pmr_vector<size_t>> LZ4Segment<T>::offsets() const {
  return _offsets;
}

template <typename T>
int LZ4Segment<T>::compressed_size() const {
  return _compressed_size;
}

template <typename T>
int LZ4Segment<T>::decompressed_size() const {
  return _decompressed_size;
}

template <typename T>
std::shared_ptr<const pmr_vector<char>> LZ4Segment<T>::dictionary() const {
  return _dictionary;
}

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
  PerformanceWarning("LZ4::get_typed_value: decompressing the whole LZ4 segment");
  auto decompressed_segment = decompress();

  const auto is_null = (*_null_values)[chunk_offset];
  if (is_null) {
    return std::nullopt;
  }

  return (*decompressed_segment)[chunk_offset];
}

template <typename T>
size_t LZ4Segment<T>::size() const {
  return _num_elements;
}

template <typename T>
std::shared_ptr<std::vector<T>> LZ4Segment<T>::decompress_with_point_access() const {
  auto decompressed_data = std::make_shared<std::vector<T>>(_decompressed_size / sizeof(T));
  const int decompressed_result = LZ4_decompress_safe(_compressed_data->data(),
                                                      reinterpret_cast<char*>(decompressed_data->data()),
                                                      _compressed_size, _decompressed_size);
  if (decompressed_result <= 0) {
    Fail("LZ4 decompression failed");
  }

  return decompressed_data;
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

template <typename T>
std::shared_ptr<std::vector<T>> LZ4Segment<T>::decompress(const std::shared_ptr<const PosList>& position_filter) const {
  auto decompressed_data = std::make_shared<std::vector<T>>(_decompressed_size / sizeof(T));

  const int block_size = 4096;
  const int num_blocks = static_cast<int>(_offsets->size());
  const std::vector<int> decompressed_blocks{};

  LZ4_streamDecode_t stream_decode;
  const auto stream_decode_ptr = std::make_unique<LZ4_streamDecode_t>(stream_decode);

  for (RowID rowID : *position_filter) {
    const auto position = rowID.chunk_offset * sizeof(T);
    const int block_id = position / block_size;
    if (std::find(decompressed_blocks.begin(), decompressed_blocks.end(), block_id) != decompressed_blocks.end()) {
      continue; // block already decompressed, skip
    }
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

    auto data_ptr = reinterpret_cast<char *>(decompressed_data->data()) + (block_id * block_size);
    std::memcpy(data_ptr, decompressed_block.data(), static_cast<size_t>(decompressed_block_size));
    decompressed_block.emplace_back(block_id);
  }

  return decompressed_data;
}

template <>
std::shared_ptr<std::vector<std::string>> LZ4Segment<std::string>::decompress() const {
  auto decompressed_data = std::make_shared<std::vector<char>>(_decompressed_size);
  const int decompressed_result = LZ4_decompress_safe(_compressed_data->data(), decompressed_data->data(),
                                                      _compressed_size, _decompressed_size);
  if (decompressed_result <= 0) {
    Fail("LZ4 decompression failed");
  }

  auto string_data = std::make_shared<std::vector<std::string>>();
  for (auto it = _offsets->cbegin(); it != _offsets->cend(); ++it) {
    auto begin = *it;
    size_t end;
    if (it + 1 == _offsets->cend()) {
      end = static_cast<size_t>(_decompressed_size);
    } else {
      end = *(it + 1);
    }

    std::string current_element;
    const auto data_begin = decompressed_data->cbegin() + begin;
    const auto data_end = decompressed_data->cbegin() + end;
    for (auto data_it = data_begin; data_it != data_end; ++data_it) {
      current_element += (*data_it);
    }
    string_data->emplace_back(current_element);
  }

  return string_data;
}

template <typename T>
std::shared_ptr<BaseSegment> LZ4Segment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_compressed_data = pmr_vector<char>{*_compressed_data, alloc};
  auto new_null_values = pmr_vector<bool>{*_null_values, alloc};

  std::shared_ptr<pmr_vector<size_t>> new_offsets_ptr = nullptr;
  if (_offsets != nullptr) {
    auto new_offsets = pmr_vector<size_t>{*_offsets, alloc};
    new_offsets_ptr = std::allocate_shared<pmr_vector<size_t>>(alloc, std::move(new_offsets));
  }
  auto new_compressed_data_ptr = std::allocate_shared<pmr_vector<char>>(alloc, std::move(new_compressed_data));
  auto new_null_values_ptr = std::allocate_shared<pmr_vector<bool>>(alloc, std::move(new_null_values));

  return std::allocate_shared<LZ4Segment>(alloc, new_compressed_data_ptr, new_null_values_ptr, new_offsets_ptr,
                                          _decompressed_size, _compressed_size, _num_elements, _dictionary);
}

template <typename T>
size_t LZ4Segment<T>::estimate_memory_usage() const {
  return static_cast<size_t>(_compressed_size);
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
