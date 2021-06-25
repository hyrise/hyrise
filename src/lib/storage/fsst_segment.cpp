#include "fsst_segment.hpp"

#include <climits>
#include <sstream>
#include <string>
#include <libfsst.hpp>
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
    _null_values = std::nullopt;
    _encoder = nullptr;
    return;
  }

  // our temporary data structure keeping char pointer and their length
  std::vector<unsigned long> row_lengths;
  std::vector<unsigned char*> row_pointers;
  row_lengths.reserve(values.size());
  row_pointers.reserve(values.size());

  // needed for compression
  _compressed_value_lengths.resize(values.size());
  _compressed_value_pointers.resize(values.size());

  unsigned total_length = 0;

  for (pmr_string& value : values) {
    total_length += value.size();
    row_lengths.push_back(value.size());
    row_pointers.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data())));  // TODO: value.c_str()
  }

  _compressed_values.resize(16 + 2 * total_length);  // why 16? need to find out
  // create symbol table
  _encoder = fsst_create(values.size(), row_lengths.data(), row_pointers.data(), 0);
  size_t compressed_size =
      fsst_compress(_encoder, values.size(), row_lengths.data(), row_pointers.data(), _compressed_values.size(),
                    _compressed_values.data(), _compressed_value_lengths.data(), _compressed_value_pointers.data());

  _compressed_values.resize(compressed_size);

  _decoder = fsst_decoder(_encoder);
  // TODO (anyone): shrink the size of _compressed_values
}

template <typename T>
FSSTSegment<T>::FSSTSegment(pmr_vector<unsigned char>& compressed_values,
                            pmr_vector<unsigned long>& compressed_value_lengths,
                            pmr_vector<unsigned char*>& compressed_value_pointers,
                            std::optional<pmr_vector<bool>>& null_values, fsst_encoder_t* encoder,
                            fsst_decoder_t& decoder)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()},
      _compressed_values{std::move(compressed_values)},
      _compressed_value_lengths{std::move(compressed_value_lengths)},
      _compressed_value_pointers{std::move(compressed_value_pointers)},
      _null_values{std::move(null_values)},
      _encoder{encoder},
      _decoder{std::move(decoder)} {}

template <typename T>
AllTypeVariant FSSTSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  // TODO(anyone): what if value is null

  const auto typed_value = get_typed_value(chunk_offset);
  if (typed_value) {
    return typed_value.value();
  }
  return NULL_VALUE;
}

template <typename T>
FSSTSegment<T>::~FSSTSegment() {
  if (_encoder != nullptr){
    fsst_destroy(_encoder);
  }
}

template <typename T>
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  if (_null_values) {
    if (_null_values.value()[chunk_offset]) {
      return std::nullopt;
    }
  }
  size_t output_size = _compressed_value_lengths[chunk_offset] * 8;  // TODO (anyone): is this correct?
  std::vector<unsigned char> output_buffer(output_size);
  size_t output_size_after_decompression =
      fsst_decompress(&_decoder, _compressed_value_lengths[chunk_offset], _compressed_value_pointers[chunk_offset],
                      output_size, output_buffer.data());
  output_buffer.resize(output_size_after_decompression);

  pmr_string output{output_buffer.begin(), output_buffer.end()};
  return {output};
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  return static_cast<ChunkOffset>(_compressed_value_pointers.size());
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO add real values

  //Dictionary segment
  //  const PolymorphicAllocator<size_t>& alloc) const {
  //    auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  //    auto new_dictionary = std::make_shared<pmr_vector<T>>(*_dictionary, alloc);
  //    auto copy = std::make_shared<DictionarySegment<T>>(std::move(new_dictionary), std::move(new_attribute_vector));
  //    copy->access_counter = access_counter;
  //    return copy;
  //  }

  // lz4
  auto new_null_values =
      _null_values ? std::optional<pmr_vector<bool>>{pmr_vector<bool>{*_null_values, alloc}} : std::nullopt;
  auto new_compressed_values = pmr_vector<unsigned char>{_compressed_values, alloc};
  auto new_compressed_value_lengths = pmr_vector<unsigned long>{_compressed_value_lengths, alloc};
  auto new_compressed_value_pointers = pmr_vector<unsigned char*>{_compressed_value_pointers, alloc};

  fsst_encoder_t* new_encoder = fsst_duplicate(_encoder);
  fsst_decoder_t new_decoder = _decoder;

  auto new_segment =
      std::make_shared<FSSTSegment>(new_compressed_values, new_compressed_value_lengths, new_compressed_value_pointers,
                                    new_null_values, new_encoder, new_decoder);

  return std::dynamic_pointer_cast<AbstractSegment>(new_segment);
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {

  // enum class MemoryUsageCalculationMode { Sampled, Full };

  auto compressed_values_size = _compressed_values.size() * sizeof(unsigned char);
  auto value_lengths_size = _compressed_value_lengths.size() * sizeof(unsigned long);
  auto value_pointers_size = _compressed_value_pointers.size() * sizeof(unsigned char*);
  auto null_value_size =  (_null_values.has_value()? _null_values.value().size() * sizeof(bool) : size_t{0}) + sizeof(std::optional<pmr_vector<bool>>);

//  auto dereferrenced_encoder = *(std::static_pointer_cast<Encoder*>(_encoder));
  auto encoder_size = sizeof(Encoder) + sizeof(SymbolTable);

  auto decoder_size = sizeof(fsst_decoder_t);
  // TODO (anyone): which memory usage should we return? compressed data or all memory
  return compressed_values_size + value_lengths_size + value_pointers_size + null_value_size + encoder_size + decoder_size;
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

// EXPLICITLY_INSTANTIATE_DATA_TYPES(FSSTSegment); TODO (anyone): do we need this?
template class FSSTSegment<pmr_string>;

}  // namespace opossum
