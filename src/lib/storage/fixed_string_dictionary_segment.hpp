#pragma once

#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "fixed_string_dictionary_segment/fixed_string_vector.hpp"
#include "types.hpp"
#include "vector_compression/base_compressed_vector.hpp"

namespace opossum {

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
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                                        const ValueID null_value_id);

  // returns the dictionary as pmr_vector
  std::shared_ptr<const pmr_vector<pmr_string>> dictionary() const;

  // returns an underlying dictionary
  std::shared_ptr<const FixedStringVector> fixed_string_dictionary() const;

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

  const ValueID null_value_id() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const FixedStringVector> _dictionary;
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  const ValueID _null_value_id;
  const std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

}  // namespace opossum
