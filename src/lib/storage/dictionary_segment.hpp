#pragma once

#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Segment implementing dictionary encoding
 *
 * Uses vector compression schemes for its attribute vector.
 */
template <typename T>
class DictionarySegment : public BaseDictionarySegment {
 public:
  explicit DictionarySegment(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                             const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                             const ValueID null_value_id);

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<T>> dictionary() const;

  /**
   * @defgroup BaseSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    const auto value_id = _decompressor->get(chunk_offset);
    if (value_id == _null_value_id) {
      return std::nullopt;
    }
    return (*_dictionary)[value_id];
  }

  ChunkOffset size() const final;

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

  // Returns the first value ID that refers to a value >= the search value and INVALID_VALUE_ID if all values are
  // smaller than the search value. Here, INVALID_VALUE_ID does not represent NULL (which isn't stored in the
  // dictionary anyway). Imagine a segment with values from 1 to 10. A scan for `WHERE a < 12` would retrieve
  // `lower_bound(12) == INVALID_VALUE_ID` and compare all values in the attribute vector to `< INVALID_VALUE_ID`.
  // Thus, returning INVALID_VALUE_ID makes comparisons much easier. However, the caller has to make sure that
  // NULL values stored in the attribute vector (stored with a value ID of unique_values_count()) are excluded.
  // See #1471 for a deeper discussion.
  ValueID lower_bound(const AllTypeVariant& value) const final;

  // Returns the first value ID that refers to a value > the search value and INVALID_VALUE_ID if all values are
  // smaller than or equal to the search value (see also lower_bound).
  ValueID upper_bound(const AllTypeVariant& value) const final;

  AllTypeVariant value_of_value_id(const ValueID value_id) const final;

  ValueID::base_type unique_values_count() const final;

  std::shared_ptr<const BaseCompressedVector> attribute_vector() const final;

  ValueID null_value_id() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _dictionary;
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  const ValueID _null_value_id;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

}  // namespace opossum
