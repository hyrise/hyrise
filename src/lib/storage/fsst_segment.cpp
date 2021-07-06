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
FSSTSegment<T>::FSSTSegment(pmr_vector<pmr_string>& values, std::optional<pmr_vector<bool>> null_values)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()}, _null_values{null_values} {
  if (values.size() == 0) {
    _compressed_offsets.resize(1);
    _null_values = std::nullopt;
    return;
  }

  // our temporary data structure keeping char pointer and their length
  std::vector<unsigned long> row_lengths;
  std::vector<unsigned char*> row_pointers;
  row_lengths.reserve(values.size());
  row_pointers.reserve(values.size());

  // needed for compression
  pmr_vector<unsigned long> compressed_value_lengths;
  pmr_vector<unsigned char*> compressed_value_pointers;
  compressed_value_lengths.resize(values.size());
  compressed_value_pointers.resize(values.size());

  _compressed_offsets.resize(values.size() + 1);

  unsigned total_length = 0;

  for (pmr_string& value : values) {
    total_length += value.size();
    row_lengths.push_back(value.size());
    row_pointers.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));  // TODO: value.c_str()
  }

  _compressed_values.resize(16 + 2 * total_length);  // why 16? need to find out
  // create symbol table
  fsst_encoder_t* encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);

  //  size_t number_compressed_strings = TODO(anyone): avoid error about unused variable in release mode
  fsst_compress(encoder, values.size(), row_lengths.data(), row_pointers.data(), _compressed_values.size(),
                _compressed_values.data(), compressed_value_lengths.data(), compressed_value_pointers.data());

  //  DebugAssert(number_compressed_strings == values.size(), "Compressed values buffer size was not big enough");

  _compressed_offsets[0] = 0;
  unsigned long aggregated_offset_sum = 0;
  size_t compressed_values_size = compressed_value_lengths.size();
  for (size_t index{1}; index <= compressed_values_size; ++index) {
    aggregated_offset_sum += compressed_value_lengths[index - 1];
    _compressed_offsets[index] = aggregated_offset_sum;
  }

  _compressed_values.resize(aggregated_offset_sum);
  _decoder = fsst_decoder(encoder);

  fsst_destroy(encoder);
}

template <typename T>
FSSTSegment<T>::FSSTSegment(pmr_vector<unsigned char>& compressed_values, pmr_vector<unsigned long>& compressed_offsets,
                            std::optional<pmr_vector<bool>>& null_values, fsst_decoder_t& decoder)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()},
      _compressed_values{std::move(compressed_values)},
      _compressed_offsets{std::move(compressed_offsets)},
      _null_values{std::move(null_values)},
      _decoder{std::move(decoder)} {}

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
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (_null_values) {
    if (_null_values.value()[chunk_offset]) {
      return std::nullopt;
    }
  }

  auto compressed_length = _compressed_offsets[chunk_offset + 1] - _compressed_offsets[chunk_offset];
  auto compressed_pointer = const_cast<unsigned char*>(
      _compressed_values.data() +
      _compressed_offsets[chunk_offset]);  //Note: we use const_cast in order to use fsst_decompress

  size_t output_size = compressed_length * 8;  // TODO (anyone): is this correct?

  //  size_t output_size = _compressed_value_lengths[chunk_offset] * 8;  // TODO (anyone): is this correct?
  std::vector<unsigned char> output_buffer(output_size);

  size_t output_size_after_decompression =
      fsst_decompress(&_decoder, compressed_length, compressed_pointer, output_size, output_buffer.data());

  output_buffer.resize(output_size_after_decompression);

  pmr_string output{output_buffer.begin(), output_buffer.end()};
  return {output};
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  return static_cast<ChunkOffset>(_compressed_offsets.size() - 1);
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_null_values =
      _null_values ? std::optional<pmr_vector<bool>>{pmr_vector<bool>{*_null_values, alloc}} : std::nullopt;
  auto new_compressed_values = pmr_vector<unsigned char>{_compressed_values, alloc};
  auto new_compressed_offsets = pmr_vector<unsigned long>{_compressed_offsets, alloc};

  fsst_decoder_t new_decoder = _decoder;

  auto new_segment =
      std::make_shared<FSSTSegment>(new_compressed_values, new_compressed_offsets, new_null_values, new_decoder);

  return std::dynamic_pointer_cast<AbstractSegment>(new_segment);
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {
  // enum class MemoryUsageCalculationMode { Sampled, Full };

  auto compressed_values_size = _compressed_values.size() * sizeof(unsigned char);
  auto compressed_offsets_size = _compressed_offsets.size() * sizeof(unsigned long);

  auto null_value_size = (_null_values.has_value() ? _null_values.value().size() * sizeof(bool) : size_t{0}) +
                         sizeof(std::optional<pmr_vector<bool>>);

  auto decoder_size = sizeof(fsst_decoder_t);
  // TODO (anyone): which memory usage should we return? compressed data or all memory
  return compressed_values_size + compressed_offsets_size + null_value_size + decoder_size;
}

template <typename T>
EncodingType FSSTSegment<T>::encoding_type() const {
  return EncodingType::FSST;
}

template <typename T>
std::optional<CompressedVectorType> FSSTSegment<T>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

template <typename T>
const fsst_decoder_t& FSSTSegment<T>::decoder() const {
  // TODO: check if arrays are copied
  return _decoder;
}

template <typename T>
const pmr_vector<unsigned char>& FSSTSegment<T>::compressed_values() const {
  return _compressed_values;
}

template <typename T>
const pmr_vector<unsigned long>& FSSTSegment<T>::compressed_offsets() const {
  return _compressed_offsets;
}

template <typename T>
const std::optional<pmr_vector<bool>>& FSSTSegment<T>::null_values() const {
  return _null_values;
}

// EXPLICITLY_INSTANTIATE_DATA_TYPES(FSSTSegment); TODO (anyone): do we need this?
template class FSSTSegment<pmr_string>;

}  // namespace opossum
