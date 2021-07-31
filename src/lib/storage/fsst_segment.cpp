#include "fsst_segment.hpp"

#include <climits>
#include <libfsst.hpp>
#include <sstream>
#include <string>
#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

#include <iostream>

namespace opossum {

template <typename T>
FSSTSegment<T>::FSSTSegment(pmr_vector<unsigned char>& compressed_values,
                            std::unique_ptr<const BaseCompressedVector>& compressed_offsets,
                            pmr_vector<uint64_t>& reference_offsets, std::optional<pmr_vector<bool>>& null_values,
                            uint64_t number_elements_per_reference_bucket, fsst_decoder_t& decoder)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()},
      _compressed_values{std::move(compressed_values)},
      _compressed_offsets{std::move(compressed_offsets)},
      _reference_offsets{std::move(reference_offsets)},
      _null_values{std::move(null_values)},
      _number_elements_per_reference_bucket{number_elements_per_reference_bucket},
      _decoder{std::move(decoder)},
      _offset_decompressor{std::move(_compressed_offsets->create_base_decompressor())} {}

template <typename T>
AllTypeVariant FSSTSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (typed_value) {
    return typed_value.value();
  }
  return NULL_VALUE;
}

template <typename T>
uint64_t FSSTSegment<T>::get_offset(const ChunkOffset chunk_offset) const {
  // For the first bucket we don't use reference offsets.
  if (chunk_offset < _number_elements_per_reference_bucket) {
    return _offset_decompressor->get(chunk_offset);
  }
  // For the number of values below 8 (number of reference offsets) we don't use reference offsets;
  // chunk_offset of 0 has the offset of 0.
  if (_number_elements_per_reference_bucket == 0 || chunk_offset == 0) {
    return _offset_decompressor->get(chunk_offset);
  }
  // Calculate the corresponding reference offset index for the chunk_offset.
  auto reference_offset_index = (chunk_offset / _number_elements_per_reference_bucket) - 1;
  // Remove the "zig-zag" pattern and return the original offset.
  return _offset_decompressor->get(chunk_offset) + _reference_offsets[reference_offset_index];
}

template <typename T>
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (_null_values) {
    if (_null_values.value()[chunk_offset]) {
      return std::nullopt;
    }
  }

  // Calculate real offset with help of reference offset vector.
  size_t real_offset = get_offset(chunk_offset);
  size_t real_offset_next = get_offset(chunk_offset + 1);
  size_t compressed_length = real_offset_next - real_offset;

  // Note: we use const_cast in order to use fsst_decompress.
  auto compressed_pointer = const_cast<unsigned char*>(_compressed_values.data() + real_offset);

  // Since the max symbol length is 8, max uncompressed size is 8 * compressed_length.
  size_t output_size = compressed_length * 8;
  std::vector<unsigned char> output_buffer(output_size);

  // Third party library call
  size_t output_size_after_decompression =
      fsst_decompress(&_decoder, compressed_length, compressed_pointer, output_size, output_buffer.data());

  // "shrink_to_fit" for output buffer
  output_buffer.resize(output_size_after_decompression);

  pmr_string output{output_buffer.begin(), output_buffer.end()};
  return {output};
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  // Compressed offsets have size of values.size() + 1 (see fsst_encoder.hpp).
  return static_cast<ChunkOffset>(_compressed_offsets->size() - 1);
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_null_values =
      _null_values ? std::optional<pmr_vector<bool>>{pmr_vector<bool>{*_null_values, alloc}} : std::nullopt;
  auto new_compressed_values = pmr_vector<unsigned char>{_compressed_values, alloc};
  auto new_compressed_offsets = _compressed_offsets->copy_using_allocator(alloc);
  auto new_reference_offsets = pmr_vector<uint64_t>{_reference_offsets, alloc};
  fsst_decoder_t new_decoder = _decoder;

  auto copy = std::make_shared<FSSTSegment>(new_compressed_values, new_compressed_offsets, new_reference_offsets,
                                            new_null_values, _number_elements_per_reference_bucket, new_decoder);
  copy->access_counter = access_counter;

  return copy;
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {
  auto compressed_values_size = _compressed_values.size() * sizeof(unsigned char);
  auto reference_offsets_size = _reference_offsets.size() * sizeof(uint64_t);
  auto compressed_offsets_size = _compressed_offsets->data_size();
  auto null_value_size = (_null_values.has_value() ? _null_values->capacity() / CHAR_BIT : size_t{0});

  return sizeof(*this) + compressed_values_size + compressed_offsets_size + reference_offsets_size + null_value_size;
}

template <typename T>
EncodingType FSSTSegment<T>::encoding_type() const {
  return EncodingType::FSST;
}

template <typename T>
std::optional<CompressedVectorType> FSSTSegment<T>::compressed_vector_type() const {
  return _compressed_offsets->type();
}

template <typename T>
const fsst_decoder_t& FSSTSegment<T>::decoder() const {
  return _decoder;
}

template <typename T>
const pmr_vector<unsigned char>& FSSTSegment<T>::compressed_values() const {
  return _compressed_values;
}

template <typename T>
const std::unique_ptr<const BaseCompressedVector>& FSSTSegment<T>::compressed_offsets() const {
  return _compressed_offsets;
}

template <typename T>
const std::optional<pmr_vector<bool>>& FSSTSegment<T>::null_values() const {
  return _null_values;
}

template <typename T>
uint64_t FSSTSegment<T>::number_elements_per_reference_bucket() const {
  return _number_elements_per_reference_bucket;
}

template <typename T>
const pmr_vector<uint64_t>& FSSTSegment<T>::reference_offsets() const {
  return _reference_offsets;
}

template class FSSTSegment<pmr_string>;

}  // namespace opossum
