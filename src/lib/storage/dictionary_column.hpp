#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_dictionary_column.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseDictionaryColumn {
 public:
  /**
   * Creates a Dictionary column from a given dictionary and attribute vector.
   * See dictionary_compression.cpp for more.
   */
  explicit DictionaryColumn(const pmr_vector<T>&& dictionary,
                            const std::shared_ptr<BaseAttributeVector>& attribute_vector);

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override;

  // return the value at a certain position.
  const T get(const size_t i) const;

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override;

  // returns an underlying dictionary
  std::shared_ptr<const pmr_vector<T>> dictionary() const;

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const final;

  // return a generated vector of all values
  const pmr_concurrent_vector<T> materialize_values() const;

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
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override;

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override;

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const override;

  // TODO(anyone): Move this to base column once final optimization is supported by gcc
  const std::shared_ptr<pmr_vector<std::pair<RowID, T>>> materialize(
      ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets = nullptr);

 protected:
  std::shared_ptr<pmr_vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
