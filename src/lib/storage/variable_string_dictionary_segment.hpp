#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace hyrise {

class BaseCompressedVector;
class VariableStringVector;

/**
 * @brief Segment implementing variable length string encoding.
 *
 * Uses vector compression schemes for its attribute vector.
 */
template <typename T>

  requires(std::is_same_v<T, pmr_string>)
class VariableStringDictionarySegment : public BaseDictionarySegment {
 public:
  VariableStringDictionarySegment(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                  const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                                  const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector);

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<char>> dictionary() const;

  std::shared_ptr<VariableStringVector> variable_string_dictionary() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<pmr_string> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    const auto value_id = _decompressor->get(chunk_offset);
    if (value_id == null_value_id()) {
      // We do not increase SegmentAccessCounter here because we do not access the dictionary.
      return std::nullopt;
    }

    // TODO(student): Does this impact performance?
    return typed_value_of_value_id(ValueID{value_id});
  }

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

  size_t memory_usage(const MemoryUsageCalculationMode mode) const final;
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

  // Returns the first offset ID that refers to a offset >= the search offset and INVALID_VALUE_ID if all values are
  // smaller than the search offset. Here, INVALID_VALUE_ID does not represent NULL (which isn't stored in the
  // dictionary anyway). Imagine a segment with values from 1 to 10. A scan for `WHERE a < 12` would retrieve
  // `lower_bound(12) == INVALID_VALUE_ID` and compare all values in the attribute vector to `< INVALID_VALUE_ID`.
  // Thus, returning INVALID_VALUE_ID makes comparisons much easier. However, the caller has to make sure that
  // NULL values stored in the attribute vector (stored with a offset ID of unique_values_count()) are excluded.
  // See #1471 for a deeper discussion.
  ValueID lower_bound(const AllTypeVariant& value) const final;

  // Returns the first value ID that refers to a value > the search value and INVALID_VALUE_ID if all values are
  // smaller than or equal to the search value (see also lower_bound).
  ValueID upper_bound(const AllTypeVariant& value) const final;

  AllTypeVariant value_of_value_id(const ValueID value_id) const final;

  pmr_string typed_value_of_value_id(const ValueID value_id) const;

  ValueID::base_type unique_values_count() const final;

  std::shared_ptr<const BaseCompressedVector> attribute_vector() const final;

  ValueID null_value_id() const final;

  const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector() const;

  static inline std::string_view get_string(const pmr_vector<uint32_t>& offset_vector,
                                            const pmr_vector<char>& dictionary, ValueID value_id) {
    const auto offset = offset_vector[value_id];
    auto next_offset = 0;

    if (value_id >= offset_vector.size() - 1) {
      next_offset = dictionary.size();
    } else {
      next_offset = offset_vector[value_id + 1];
    }
    const auto string_length = next_offset - offset - 1;

    return std::string_view{dictionary.data() + offset, string_length};
  }

 protected:
  const std::shared_ptr<const pmr_vector<char>> _dictionary;
  // Maps chunk offsets to value ids.
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;
  // Maps value ids to dictionary offsets.
  const std::shared_ptr<const pmr_vector<uint32_t>> _offset_vector;
};

extern template class VariableStringDictionarySegment<pmr_string>;

}  // namespace hyrise
