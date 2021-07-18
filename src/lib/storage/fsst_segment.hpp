#pragma once

#include <array>
#include <memory>
#include <type_traits>

#include <fsst.h>
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
  //  explicit FSSTSegment();     // TODO: remove
  FSSTSegment(pmr_vector<unsigned char>& compressed_values,
              std::unique_ptr<const BaseCompressedVector>& compressed_offsets,
              pmr_vector<uint64_t>& reference_offsets, std::optional<pmr_vector<bool>>& null_values,
              uint64_t number_elements_per_reference_bucket, fsst_decoder_t& decoder);

      /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;

  uint64_t get_offset(const ChunkOffset chunk_offset) const;

  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  const fsst_decoder_t& decoder() const;

  const pmr_vector<unsigned char>& compressed_values() const;
  const std::unique_ptr<const BaseCompressedVector>& compressed_offsets() const;
  const std::optional<pmr_vector<bool>>& null_values() const;
  uint64_t number_elements_per_reference_bucket() const;
  const pmr_vector<uint64_t>& reference_offsets() const;

  /**@}*/

 private:
  pmr_vector<unsigned char> _compressed_values;
  std::unique_ptr<const BaseCompressedVector> _compressed_offsets;
  pmr_vector<uint64_t> _reference_offsets;
  std::optional<pmr_vector<bool>> _null_values;
  uint64_t _number_elements_per_reference_bucket;
  mutable fsst_decoder_t _decoder;
  std::unique_ptr<BaseVectorDecompressor> _offset_decompressor;
};

//EXPLICITLY_DECLARE_DATA_TYPES(FSSTSegment);
extern template class FSSTSegment<pmr_string>;

}  // namespace opossum
