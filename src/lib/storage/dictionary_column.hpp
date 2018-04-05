#pragma once

#include <memory>
#include <string>

#include "base_dictionary_column.hpp"
#include "types.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class BaseCompressedVector;

/**
 * @brief Column implementing dictionary encoding
 *
 * Uses vector compression schemes for its attribute vector.
 */
template <typename T>
class DictionaryColumn : public BaseDictionaryColumn {
 public:
  explicit DictionaryColumn(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                            const BaseCompressedVectorCSPtr& attribute_vector,
                            const ValueID null_value_id);

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<T>> dictionary() const;

  /**
   * @defgroup BaseColumn interface
   * @{
   */

  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const final;

  size_t size() const final;

  BaseColumnSPtr copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const final;

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
  ValueID lower_bound(const AllTypeVariant& value) const final;
  ValueID upper_bound(const AllTypeVariant& value) const final;

  size_t unique_values_count() const final;

  BaseCompressedVectorCSPtr attribute_vector() const final;

  const ValueID null_value_id() const final;

  /**@}*/

 protected:
  const std::shared_ptr<const pmr_vector<T>> _dictionary;
  const BaseCompressedVectorCSPtr _attribute_vector;
  const ValueID _null_value_id;
};

}  // namespace opossum
