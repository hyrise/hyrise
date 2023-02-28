#pragma once

#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "fixed_string_dictionary_segment/fixed_string_span.hpp"
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

  explicit FixedStringDictionarySegment(const std::byte* start_address);

  // returns an underlying dictionary
  std::shared_ptr<const FixedStringSpan> fixed_string_dictionary() const;

  std::shared_ptr<const FixedStringSpan> fixed_string_dictionary_span() const;

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;

  ChunkOffset size() const final;

  std::shared_ptr<AbstractSegment> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

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
  const std::shared_ptr<const FixedStringVector> _dictionary_base_vector;
  std::shared_ptr<const FixedStringSpan> _dictionary;
  std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;

  static constexpr auto ENCODING_TYPE_OFFSET_INDEX = uint32_t{0};
  static constexpr auto STRING_LENGTH_OFFSET_INDEX = uint32_t{1};
  static constexpr auto DICTIONARY_SIZE_OFFSET_INDEX = uint32_t{2};
  static constexpr auto ATTRIBUTE_VECTOR_OFFSET_INDEX = uint32_t{3};
  static constexpr auto HEADER_OFFSET_BYTES = uint32_t{16};
  static constexpr auto NUM_BYTES_32_BIT_ENCODING = uint32_t{4};
  static constexpr auto NUM_BYTES_16_BIT_ENCODING = uint32_t{2};
};

extern template class FixedStringDictionarySegment<pmr_string>;

}  // namespace hyrise
