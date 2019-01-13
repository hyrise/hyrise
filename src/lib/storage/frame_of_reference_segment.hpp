#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <type_traits>

#include <array>
#include <memory>

#include "base_encoded_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Segment implementing frame-of-reference encoding
 *
 * Frame-of-Reference encoding divides the values of segment into
 * fixed-size blocks. The values of each block are encoded
 * as an offset from the block’s minimum value. These offsets,
 * which can ideally be represented by fewer bits, are then
 * compressed using vector compression (null suppression).
 * FOR encoding on its own without vector compression does not
 * add any benefit.
 */
template <typename T, typename = std::enable_if_t<encoding_supports_data_type(
                          enum_c<EncodingType, EncodingType::FrameOfReference>, hana::type_c<T>)>>
class FrameOfReferenceSegment : public BaseEncodedSegment {
 public:
  /**
   * The segment is divided into fixed-size blocks.
   * Each block has its own minimum from which the
   * offsets are calculated. Theoretically, it would be
   * possible to make the block size dependent on the
   * data’s properties. Determining the optimal size
   * is however not trivial.
   */
  static constexpr auto block_size = 2048u;

  explicit FrameOfReferenceSegment(pmr_vector<T> block_minima, pmr_vector<bool> null_values,
                                   std::unique_ptr<const BaseCompressedVector> offset_values);

  const pmr_vector<T>& block_minima() const;
  const pmr_vector<bool>& null_values() const;
  const BaseCompressedVector& offset_values() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  size_t size() const final;

  std::shared_ptr<BaseSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 private:
  const pmr_vector<T> _block_minima;
  const pmr_vector<bool> _null_values;
  const std::unique_ptr<const BaseCompressedVector> _offset_values;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

}  // namespace opossum
