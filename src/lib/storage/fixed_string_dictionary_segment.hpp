#pragma once

#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "fixed_string_dictionary_segment/fixed_string_vector.hpp"
#include "types.hpp"
#include "vector_compression/base_compressed_vector.hpp"

namespace hyrise {

class BaseCompressedVector;

/**
 * @brief Segment implementing dictionary encoding for strings
 *
 * It compresses string segments by avoiding small string optimization.
 * Uses vector compression schemes for its attribute vector.
 */
template <typename T>
class FixedStringDictionarySegment : public BaseDictionarySegment {
 public:
  explicit FixedStringDictionarySegment(const std::shared_ptr<const FixedStringVector>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector);

  // returns an underlying dictionary
  std::shared_ptr<const FixedStringVector> fixed_string_dictionary() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_memory_resource(MemoryResource& memory_resource) const final;

  size_t memory_usage(const MemoryUsageCalculationMode /*mode*/ = MemoryUsageCalculationMode::Full) const final;
  /**@}*/

  /**
   * @defgroup AbstractEncodedSegment interface
   * @{
   */
  std::optional<CompressedVectorType> compressed_vector_type() const final;
  /**@}*/

  /**
   * @defgroup BaseDictionarySegment interface
   * @{
   */
  EncodingType encoding_type() const final;

  ValueID lower_bound(const AllTypeVariant& value) const final;
  ValueID upper_bound(const AllTypeVariant& value) const final;

  AllTypeVariant value_of_value_id(const ValueID value_id) const final;

  ValueID::base_type unique_values_count() const final;

  std::shared_ptr<const BaseCompressedVector> attribute_vector() const final;

  ValueID null_value_id() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const FixedStringVector> _dictionary;
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  const std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

extern template class FixedStringDictionarySegment<pmr_string>;

}  // namespace hyrise
