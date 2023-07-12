#pragma once

#include <memory>
#include <string>

#include "base_dictionary_segment.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace hyrise {

class BaseCompressedVector;

/**
 * @brief Segment implementing variable length string encoding.
 *
 * Uses vector compression schemes for its attribute vector.
 */
template<typename T>
class VariableStringDictionarySegment : public AbstractEncodedSegment {
 public:
  VariableStringDictionarySegment(const std::shared_ptr<const pmr_vector<char>>& dictionary,
                                  const std::shared_ptr<const BaseCompressedVector>& attribute_vector);

  // TODO:: remove - use in binary Writer
  VariableStringDictionarySegment() : AbstractEncodedSegment(DataType::String) {};

  /**
   * @defgroup AbstractSegment interface
   * @{
   */

  AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  std::optional<pmr_string> get_typed_value(const ChunkOffset chunk_offset) const {
    // performance critical - not in cpp to help with inlining
    const auto offset = _decompressor->get(chunk_offset);
    if (offset == _klotz->size()) {
      return std::nullopt;
    }

    return pmr_string(_klotz->data() + offset);
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

  const std::shared_ptr<const BaseCompressedVector> &attribute_vector() const;
  const std::shared_ptr<const pmr_vector<char>> &klotz() const;

 protected:
  const std::shared_ptr<const pmr_vector<char>> _klotz;
  // Maps chunk offsets to value ids.
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  std::unique_ptr<BaseVectorDecompressor> _decompressor;
};

extern template class VariableStringDictionarySegment<pmr_string>;

}  // namespace hyrise
