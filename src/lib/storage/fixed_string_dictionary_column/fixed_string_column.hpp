#pragma once

#include <memory>
#include <string>

#include "../base_dictionary_column.hpp"
#include "fixed_string_vector.hpp"
#include "types.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Column implementing dictionary encoding for strings
 *
 * It compresses string columns by avoiding small string optimization.
 * Uses vector compression schemes for its attribute vector.
 */
template <typename T>
class FixedStringColumn : public BaseDictionaryColumn {
 public:
  explicit FixedStringColumn(const std::shared_ptr<const FixedStringVector>& dictionary,
                             const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                             const ValueID null_value_id);

  // returns the dictionary as pmr_vector
  std::shared_ptr<const pmr_vector<std::string>> dictionary() const;

  // returns an underlying dictionary
  std::shared_ptr<const FixedStringVector> fixed_string_dictionary() const;

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
  CompressedVectorType compressed_vector_type() const final;
  /**@}*/

  /**
   * @defgroup BaseDictionaryColumn interface
   * @{
   */
  EncodingType encoding_type() const final;

  ValueID lower_bound(const AllTypeVariant& value) const final;
  ValueID upper_bound(const AllTypeVariant& value) const final;

  size_t unique_values_count() const final;

  std::shared_ptr<const BaseCompressedVector> attribute_vector() const final;

  const ValueID null_value_id() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const FixedStringVector> _dictionary;
  const std::shared_ptr<const BaseCompressedVector> _attribute_vector;
  const ValueID _null_value_id;
};

}  // namespace opossum
