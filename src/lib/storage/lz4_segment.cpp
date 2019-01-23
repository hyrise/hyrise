#include "lz4_segment.hpp"

#include "lib/lz4.h"
#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
LZ4Segment<T>::LZ4Segment(const int decompressed_size, const int max_compressed_size,
                             std::shared_ptr<std::vector<char>> compressed_data)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _decompressed_size{decompressed_size},
      _max_compressed_size{max_compressed_size},
      _compressed_data{std::move(compressed_data)} {}

template <typename T>
int LZ4Segment<T>::decompressed_size() const {
  return _decompressed_size;
}

template <typename T>
int LZ4Segment<T>::max_compressed_size() const {
  return _max_compressed_size;
}

template <typename T>
const std::vector<char>& LZ4Segment<T>::compressed_data() const {
  return *_compressed_data;
}

template <typename T>
const AllTypeVariant LZ4Segment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  PerformanceWarning("LZ4::operator[]: decompressing the whole LZ4 segment");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  auto decompressed_segment = decompress();
  return AllTypeVariant{(*decompressed_segment)[chunk_offset]};
}

template <typename T>
const std::optional<T> LZ4Segment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  PerformanceWarning("LZ4::get_typed_value: decompressing the whole LZ4 segment");
  auto decompressed_segment = decompress();
  return (*decompressed_segment)[chunk_offset];
}

template <typename T>
size_t LZ4Segment<T>::size() const {
  return _compressed_data->size();
}

template <typename T>
std::shared_ptr<std::vector<T>> LZ4Segment<T>::decompress() const {
  auto decompressed_data = std::make_shared<std::vector<T>>(_decompressed_size / sizeof(T));
  int compressed_size = static_cast<int>(_compressed_data->size());
  const int decompressed_result = LZ4_decompress_safe(_compressed_data->data(),
                                                      reinterpret_cast<char*>(decompressed_data->data()),
                                                      compressed_size, _decompressed_size);
  if (decompressed_result <= 0) {
    throw std::runtime_error("LZ4 decompression failed");
  }

  return decompressed_data;
}

template <>
std::shared_ptr<std::vector<std::string>> LZ4Segment<std::string>::decompress() const {
  auto decompressed_data = std::make_shared<std::vector<char>>(_decompressed_size);
  int compressed_size = static_cast<int>(_compressed_data->size());
  const int decompressed_result = LZ4_decompress_safe(_compressed_data->data(),
                                                      decompressed_data->data(),
                                                      compressed_size, _decompressed_size);
  if (decompressed_result <= 0) {
    throw std::runtime_error("LZ4 decompression failed");
  }

  auto string_data = std::make_shared<std::vector<std::string>>();
  std::string current_element = "";
  for (const auto& element : (*decompressed_data)) {
    if (element == '\0') {
      string_data->emplace_back(current_element);
      current_element = "";
      continue;
    }
    current_element += element;
  }

  return string_data;
}

template <typename T>
std::shared_ptr<BaseSegment> LZ4Segment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  return std::allocate_shared<LZ4Segment>(alloc, _decompressed_size, _max_compressed_size, std::move(_compressed_data));
}

template <typename T>
size_t LZ4Segment<T>::estimate_memory_usage() const {
  return static_cast<size_t>(_max_compressed_size);
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
