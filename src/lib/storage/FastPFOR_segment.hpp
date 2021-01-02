#pragma once

#include <memory>

#include "abstract_encoded_segment.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

template <typename T>
class FastPFORSegment : public AbstractEncodedSegment {
 public:
  explicit FastPFORSegment(const std::shared_ptr<const pmr_vector<T>>& values,
                           const std::shared_ptr<const pmr_vector<bool>>& null_values);

  std::shared_ptr<const pmr_vector<T>> values() const;
  std::shared_ptr<const pmr_vector<bool>> null_values() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining

    return 0;
  }

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

 protected:
  const std::shared_ptr<const pmr_vector<T>> _values;
  const std::shared_ptr<const pmr_vector<bool>> _null_values;
};

}  // namespace opossum
