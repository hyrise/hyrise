#pragma once

#include <boost/hana/contains.hpp>
#include <boost/hana/tuple.hpp>
#include <boost/hana/type.hpp>

#include <array>
#include <memory>

#include "base_encoded_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Column implementing frame-of-reference encoding
 */
template <typename T>
class FrameOfReferenceColumn : public BaseEncodedColumn {
 public:
  static constexpr auto frame_size = 2048u;

  explicit FrameOfReferenceColumn(std::shared_ptr<const pmr_vector<T>> reference_frames,
                                  std::shared_ptr<const BaseCompressedVector> offset_values,
                                  std::shared_ptr<const pmr_vector<bool>> null_values);

  std::shared_ptr<const pmr_vector<T>> reference_frames() const;
  std::shared_ptr<const BaseCompressedVector> offset_values() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;

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
  const std::shared_ptr<const pmr_vector<T>> _reference_frames;
  const std::shared_ptr<const BaseCompressedVector> _offset_values;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
};

}  // namespace opossum
