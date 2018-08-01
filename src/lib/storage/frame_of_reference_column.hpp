#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <type_traits>

#include <array>
#include <memory>

#include "base_encoded_column.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Column implementing frame-of-reference encoding
 *
 * Frame-of-Reference encoding divides the values of column into
 * fixed-size blocks. The values of each block are encoded
 * as an offset from the block’s minimum value. These offsets,
 * which can ideally be represented by fewer bits, are then
 * compressed using vector compression (null suppression).
 * FOR encoding on its own without vector compression does not
 * add any benefit.
 */
template <typename T, typename = std::enable_if_t<encoding_supports_data_type(
                          enum_c<EncodingType, EncodingType::FrameOfReference>, hana::type_c<T>)>>
class FrameOfReferenceColumn : public BaseEncodedColumn {
 public:
  /**
   * The column is divided into fixed-size blocks.
   * Each block has its own minimum from which the
   * offsets are calculated. Theoretically, it would
   * possible to make the block size dependent on the
   * data’s properties. Determining the optimal size
   * is however not trivial.
   */
  static constexpr auto block_size = 2048u;

  explicit FrameOfReferenceColumn(pmr_vector<T> block_minima, pmr_vector<bool> null_values,
                                  std::unique_ptr<const BaseCompressedVector> offset_values);

  const pmr_vector<T>& block_minima() const;
  const pmr_vector<bool>& null_values() const;
  const BaseCompressedVector& offset_values() const;

  /**
   * @defgroup BaseColumn interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  size_t size() const final;

  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t estimate_memory_usage() const final;

  /**@}*/

  /**
   * @defgroup BaseEncodedColumn interface
   * @{
   */

  EncodingType encoding_type() const final;
  CompressedVectorType compressed_vector_type() const final;

  /**@}*/

 private:
  const pmr_vector<T> _block_minima;
  const pmr_vector<bool> _null_values;
  const std::unique_ptr<const BaseCompressedVector> _offset_values;
  std::unique_ptr<BaseVectorDecompressor> _decoder;
};

}  // namespace opossum
