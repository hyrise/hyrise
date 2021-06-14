#include "fsst_segment.hpp"

#include <climits>
#include <sstream>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
FSSTSegment<T>::FSSTSegment(pmr_vector<pmr_string> values, pmr_vector<bool> null_values)
    : AbstractEncodedSegment{data_type_from_type<pmr_string>()},
    _values{std::move(values)}, _null_values{std::move(null_values) {

      std::vector<unsigned char> output_buffer(output_size);

    //---- playground

          // our temporary data structure for holding
      std::vector<unsigned long> row_lengths;       // row_l
      std::vector<unsigned long> compressed_row_lengths;      // compressedRowLens

      // needed for symbol table creation
      std::vector<unsigned char*> row_pointerscompressed_row_lengths;
      std::vector<unsigned char*> compressed_row_pointers;

      row_lengths.reserve(_values.size());
      row_pointers.reserve(_values.size());
      compressed_row_lengths.resize(_values.size());
      compressed_row_pointers.resize(_values.size());

      unsigned total_length = 0;  //

      for (std::string& value : values) {
        totalLen += value.size();
        row_lens.push_back(value.size());
        row_ptrs.push_back(reinterpret_cast<unsigned char*>(const_cast<char*>(value.data()))); // TODO: value.c_str()
      }

      std::vector<unsigned char> compressionBuffer, fullBuffer;

      compressionBuffer.resize(16 + 2 * totalLen);

      // COMPRESSION
      fsst_encoder_t* encoder = fsst_create(values.size(), row_lens.data(), row_ptrs.data(), 0);

      unsigned char buffer[sizeof(fsst_decoder_t)];
      fsst_export(encoder, buffer);

      fsst_compress(encoder, values.size(), row_lens.data(), row_ptrs.data(), compressionBuffer.size(),
                    compressionBuffer.data(), compressedRowLens.data(), compressedRowPtrs.data());
      //  fsst_destroy(encoder);

      for (size_t i = 0; i < compressedRowPtrs.size(); i++) {
        for (size_t j = 0; j < compressedRowLens[i]; j++) {
          printf("%d ", compressedRowPtrs[i][j]);
        }
        printf("\n");
      }

      // DECOMPRESSION
      fsst_decoder_t decoder = fsst_decoder(encoder);

      size_t output_size = 6 + 1;
      std::vector<unsigned char> output_buffer(output_size);
      size_t output_size_after_decompression =
          fsst_decompress(&decoder, compressedRowLens[0], compressedRowPtrs[0], output_size, output_buffer.data());

      std::cout << output_size_after_decompression << std::endl;

      for (size_t i = 0; i < output_size_after_decompression; i++) {
        printf("%c", output_buffer[i]);
      }

      printf("\n");



      //---

      _compressed_values = pmr_vector<pmr_string>();
  }




  }


template <typename T>
AllTypeVariant FSSTSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  // TODO add real values
  return T{};

}

template <typename T>
std::optional<T> FSSTSegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  // TODO add real values
  return std::nullopt;
}

template <typename T>
ChunkOffset FSSTSegment<T>::size() const {
  // TODO add real values
  return static_cast<ChunkOffset>(0);
}

template <typename T>
std::shared_ptr<AbstractSegment> FSSTSegment<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  // TODO add real values
  return std::shared_ptr<FSSTSegment<T>>();
}

template <typename T>
size_t FSSTSegment<T>::memory_usage(const MemoryUsageCalculationMode) const {
  // TODO add real values
  return size_t{0};
}

template <typename T>
EncodingType FSSTSegment<T>::encoding_type() const {
  // TODO add real values
  return EncodingType::FSST;
}

template <typename T>
std::optional<CompressedVectorType> FSSTSegment<T>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

template <>
std::optional<CompressedVectorType> FSSTSegment<pmr_string>::compressed_vector_type() const {
  // TODO add real values
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(FSSTSegment);

}  // namespace opossum
