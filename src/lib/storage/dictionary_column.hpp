#pragma once

#include <fstream>
#include <memory>
#include <regex>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_attribute_vector.hpp"
#include "untyped_dictionary_column.hpp"

#include "types.hpp"

namespace opossum {
template <typename T>
class ValueColumn;

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public UntypedDictionaryColumn {
 public:
  // Creates a Dictionary column from a given dictionary and attribute vector.
  explicit DictionaryColumn(const std::vector<T>&& dictionary,
                            const std::shared_ptr<BaseAttributeVector>& attribute_vector)
      : _dictionary(std::make_shared<std::vector<T>>(std::move(dictionary))), _attribute_vector(attribute_vector) {}

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    /*
    Handle null values, this is only used for testing the results of joins so far.
    In order to be able to define an expected output table, we need to replace INVALID_CHUNK_OFFSET
    with some printable character, in our case 0, resp. "0".
    Since there is no constructor for String, which takes a numeric 0, we have to differentiate between numbers and
    strings.

    This should be replaced as soon as we have proper NULL values in Opossum.
    Similar code is in value_column.hpp
    */
    if (i == INVALID_CHUNK_OFFSET) {
      if (std::is_same<T, std::string>::value) {
        return "0";
      }
      return T(0);
    }
    return (*_dictionary)[_attribute_vector->get(i)];
  }

  // return the value at a certain position.
  const T get(const size_t i) const { return (*_dictionary)[_attribute_vector->get(i)]; }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override { throw std::logic_error("DictionaryColumn is immutable"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const final { return _attribute_vector; }

  // return a generated vector of all values
  const tbb::concurrent_vector<T> materialize_values() const {
    tbb::concurrent_vector<T> values(_attribute_vector->size());

    for (ChunkOffset chunk_offset = 0; chunk_offset < _attribute_vector->size(); ++chunk_offset) {
      values[chunk_offset] = (*_dictionary)[_attribute_vector->get(chunk_offset)];
    }

    return values;
  }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (it == _dictionary->cend()) return INVALID_VALUE_ID;
    return std::distance(_dictionary->cbegin(), it);
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const final {
    auto typed_value = type_cast<T>(value);
    return lower_bound(typed_value);
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (it == _dictionary->cend()) return INVALID_VALUE_ID;
    return std::distance(_dictionary->cbegin(), it);
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const final {
    auto typed_value = type_cast<T>(value);
    return upper_bound(typed_value);
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const final { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override {
    visitable.handle_dictionary_column(*this, std::move(context));
  }

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override {
    std::stringstream buffer;
    // buffering value at chunk_offset
    T value = _dictionary->at(_attribute_vector->get(chunk_offset));
    buffer << value;
    uint32_t length = buffer.str().length();
    // writing byte representation of length
    buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

    // appending the new string to the already present string
    row_string += buffer.str();
  }

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  void copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const override {
    auto& output_column = static_cast<ValueColumn<T>&>(value_column);
    auto& values_out = output_column.values();

    auto value = value_by_value_id(_attribute_vector->get(chunk_offset));
    values_out.push_back(value);
  }

  // TODO(anyone): Move this to base column once final optimization is supported by gcc
  const std::shared_ptr<std::vector<std::pair<RowID, T>>> materialize(
      ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets = nullptr) {
    auto materialized_vector = std::make_shared<std::vector<std::pair<RowID, T>>>();

    /*
    We only offset if this ValueColumn was referenced by a ReferenceColumn. Thus it might actually be filtered.
    */
    if (offsets) {
      materialized_vector->reserve(offsets->size());
      for (auto& offset : *offsets) {
        T value = (*_dictionary)[_attribute_vector->get(offset)];
        auto materialized_row = std::make_pair(RowID{chunk_id, offset}, value);
        materialized_vector->push_back(materialized_row);
      }
    } else {
      materialized_vector->reserve(_attribute_vector->size());
      for (ChunkOffset offset = 0; offset < _attribute_vector->size(); offset++) {
        T value = (*_dictionary)[_attribute_vector->get(offset)];
        auto materialized_row = std::make_pair(RowID{chunk_id, offset}, value);
        materialized_vector->push_back(materialized_row);
      }
    }

    return materialized_vector;
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};
}  // namespace opossum
