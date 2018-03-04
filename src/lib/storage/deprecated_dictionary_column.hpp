#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_deprecated_dictionary_column.hpp"
#include "types.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

/**
 * @brief Old implementation of a dictionary-encoded column
 *
 * @deprecated The new implementation (DictionaryColumn) is faster and
 *             supports more compression techniques for the attribute vector.
 *             Currently, the import and export operators are not yet updated
 *             to support the new dictionary column implementation. Once this
 *             is done, it will be removed.
 */
template <typename T>
class DeprecatedDictionaryColumn : public BaseDeprecatedDictionaryColumn {
 public:
  /**
   * Creates a Dictionary column from a given dictionary and attribute vector.
   * See deprecated_dictionary_compression.cpp for more.
   */
  explicit DeprecatedDictionaryColumn(pmr_vector<T>&& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector);

  explicit DeprecatedDictionaryColumn(const std::shared_ptr<pmr_vector<T>>& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector);

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Returns whether a value is NULL
  bool is_null(const ChunkOffset chunk_offset) const;

  // return the value at a certain position.
  // Only use if you are certain that no null values are present, otherwise an Assert fails.
  const T get(const ChunkOffset chunk_offset) const;

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<T>> dictionary() const;

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const final;

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const;

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const;

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const final;

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const;

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const final;

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const final;

  // return the number of entries
  size_t size() const override;

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) const override;

  // Copies a DeprecatedDictionaryColumn using a new allocator. This is useful for placing it on a new NUMA node.
  std::shared_ptr<BaseColumn> copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const override;

  size_t estimate_memory_usage() const override;

 protected:
  std::shared_ptr<pmr_vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
