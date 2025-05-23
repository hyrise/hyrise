#pragma once

#include <array>
#include <memory>
#include <type_traits>

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include "abstract_encoded_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace hyrise {

class BaseCompressedVector;

/**
 * @brief Segment implementing frame-of-reference encoding
 *
 * Frame-of-Reference encoding divides the values of segment into fixed-size blocks. The values of each block are
 * encoded as an offset from the block's minimum value. These offsets, which can ideally be represented by fewer bits,
 * are then compressed using vector compression (null suppression).
 *
 * FOR encoding on its own without vector compression does not add any benefit.
 *
 * Null values are stored in a separate vector. Note, for correct offset handling, the minimum of each frame is stored 
 * in the offset_values vector at each position that is NULL.
 *
 * std::enable_if_t must be used here and cannot be replaced by a static_assert in order to prevent instantiation of
 * FrameOfReferenceSegment<T> with T other than int32_t. Otherwise, the compiler might instantiate
 * FrameOfReferenceSegment with other types even if they are never actually needed.
 * "If the function selected by overload resolution can be determined without instantiating a class template
 *  definition, it is unspecified whether that instantiation actually takes place." Draft Std. N4800 12.8.1.8
 */
template <typename T, typename = std::enable_if_t<encoding_supports_data_type(
                          enum_c<EncodingType, EncodingType::FrameOfReference>, hana::type_c<T>)>>
class FrameOfReferenceSegment : public AbstractEncodedSegment {
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

  explicit FrameOfReferenceSegment(pmr_vector<T> block_minima, std::optional<pmr_vector<bool>> null_values,
                                   std::unique_ptr<const BaseCompressedVector> offset_values);

  const pmr_vector<T>& block_minima() const;
  const std::optional<pmr_vector<bool>>& null_values() const;
  const BaseCompressedVector& offset_values() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    if (_null_values && (*_null_values)[chunk_offset]) {
      return std::nullopt;
    }
    const auto minimum = _block_minima[chunk_offset / block_size];
    const auto value = static_cast<T>(_decompressor->get(chunk_offset)) + minimum;
    return value;
  }

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_memory_resource(MemoryResource& memory_resource) const final;

  size_t memory_usage(const MemoryUsageCalculationMode /*mode*/) const final;

  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */

  EncodingType encoding_type() const final;
  std::optional<CompressedVectorType> compressed_vector_type() const final;

  /**@}*/

 private:
  const pmr_vector<T> _block_minima;
  const std::optional<pmr_vector<bool>> _null_values;
  const std::unique_ptr<const BaseCompressedVector> _offset_values;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

extern template class FrameOfReferenceSegment<int32_t>;

}  // namespace hyrise
