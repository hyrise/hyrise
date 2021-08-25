#pragma once

#include <fsst.h>
#include <array>
#include <memory>
#include <type_traits>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "abstract_encoded_segment.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "storage/vector_compression/base_vector_decompressor.hpp"
#include "types.hpp"

namespace opossum {

template <typename T>
class FSSTSegment : public AbstractEncodedSegment {
 public:
  explicit FSSTSegment(pmr_vector<unsigned char>& compressed_values,
                       std::unique_ptr<const BaseCompressedVector>& compressed_offsets,
                       pmr_vector<uint64_t>& reference_offsets, std::optional<pmr_vector<bool>>& null_values,
                       uint32_t number_elements_per_reference_bucket, fsst_decoder_t& decoder);

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;

  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

  uint64_t get_offset(const ChunkOffset chunk_offset) const;

  // Getter methods
  const pmr_vector<unsigned char>& compressed_values() const;
  const std::unique_ptr<const BaseCompressedVector>& compressed_offsets() const;
  const pmr_vector<uint64_t>& reference_offsets() const;
  const std::optional<pmr_vector<bool>>& null_values() const;
  uint32_t number_elements_per_reference_bucket() const;
  const fsst_decoder_t& decoder() const;

 private:
  const pmr_vector<unsigned char> _compressed_values;
  const std::unique_ptr<const BaseCompressedVector> _compressed_offsets;
  const pmr_vector<uint64_t> _reference_offsets;
  const std::optional<pmr_vector<bool>> _null_values;
  const uint32_t _number_elements_per_reference_bucket;
  mutable fsst_decoder_t _decoder;
  const std::unique_ptr<BaseVectorDecompressor> _offset_decompressor;
};

extern template class FSSTSegment<pmr_string>;

}  // namespace opossum
