#include "lz4_segment.hpp"

#include <lz4.h>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<char>&& compressed_data, pmr_vector<bool>&& null_values,
                          pmr_vector<size_t>&& offsets, const size_t decompressed_size)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _compressed_data{std::move(compressed_data)},
      _null_values{std::move(null_values)},
      _offsets{std::move(offsets)},
      _decompressed_size{decompressed_size} {}

template <typename T>
LZ4Segment<T>::LZ4Segment(pmr_vector<char>&& compressed_data, pmr_vector<bool>&& null_values,
                          const size_t decompressed_size)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _compressed_data{std::move(compressed_data)},
      _null_values{std::move(null_values)},
      _offsets{std::nullopt},
      _decompressed_size{decompressed_size} {}

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

  const auto is_null = _null_values[chunk_offset];
  if (is_null) {
    return std::nullopt;
  }

  return decompressed_segment[chunk_offset];
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
size_t LZ4Segment<T>::size() const {
  return _null_values.size();
}

template <typename T>
std::vector<T> LZ4Segment<T>::decompress() const {
  auto decompressed_data = std::vector<T>(_decompressed_size / sizeof(T));
  auto compressed_size = static_cast<int>(_compressed_data.size());
  const int decompressed_result =
      LZ4_decompress_safe(_compressed_data.data(), reinterpret_cast<char*>(decompressed_data.data()), compressed_size,
                          static_cast<int>(_decompressed_size));
  Assert(decompressed_result > 0, "LZ4 decompression failed");

  return decompressed_data;
}

template <>
std::vector<pmr_string> LZ4Segment<pmr_string>::decompress() const {
  /**
   * If the input segment only contained empty strings the original size is 0. That can't be decompressed and instead
   * we can just return as many empty strings as the input contained.
   */
  if (!_decompressed_size) {
    return std::vector<pmr_string>(_null_values.size());
  }

  auto decompressed_data = std::vector<char>(_decompressed_size);
  auto compressed_size = static_cast<int>(_compressed_data.size());
  const int decompressed_result = LZ4_decompress_safe(_compressed_data.data(), decompressed_data.data(),
                                                      compressed_size, static_cast<int>(_decompressed_size));
  Assert(decompressed_result > 0, "LZ4 decompression failed");

  /**
   * Decode the previously encoded string data. These strings are all appended and separated along the stored offsets.
   * Each offset corresponds to a single string. The stored offset itself is the character offset of the first character
   * of the string. The end offset is the first character behind the string that is NOT part of the string (i.e., an
   * exclusive offset). It is usually the next offset in the vector. In the case of the last offset the end offset is
   * indicated by the end of the data vector.
   */
  auto decompressed_strings = std::vector<pmr_string>();
  for (auto it = _offsets->cbegin(); it != _offsets->cend(); ++it) {
    auto start_char_offset = *it;
    size_t end_char_offset;
    if (it + 1 == _offsets->cend()) {
      end_char_offset = _decompressed_size;
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
std::shared_ptr<BaseSegment> LZ4Segment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_compressed_data = pmr_vector<char>{_compressed_data, alloc};
  auto new_null_values = pmr_vector<bool>{_null_values, alloc};

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
