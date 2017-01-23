#pragma once

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <fstream>
#include <iterator>

#include "../types.hpp"
#include "base_attribute_vector.hpp"
#include "base_column.hpp"
#include "fitted_attribute_vector.hpp"
#include "value_column.hpp"

namespace opossum {

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID = std::numeric_limits<ValueID>::max();

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  explicit DictionaryColumn(std::shared_ptr<BaseColumn> base_column) {
    if (auto val_col = std::dynamic_pointer_cast<ValueColumn<T>>(base_column)) {
      // See: https://goo.gl/MCM5rr
      // Create dictionary (enforce unqiueness and sorting)
      _dictionary = val_col->values();

      std::sort(_dictionary.begin(), _dictionary.end());
      _dictionary.erase(std::unique(_dictionary.begin(), _dictionary.end()), _dictionary.end());
      _dictionary.shrink_to_fit();

      _attribute_vector = _create_fitted_attribute_vector(unique_values_count(), val_col->values().size());
      auto& values = val_col->values();

      for (ChunkOffset offset = 0; offset < values.size(); ++offset) {
        ValueID value_id = std::distance(_dictionary.cbegin(),
                                         std::lower_bound(_dictionary.cbegin(), _dictionary.cend(), values[offset]));
        _attribute_vector->set(offset, value_id);
      }
    }
  }

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    if (i == NULL_VALUE) {
      // doesn't work with string values'
      return {0};
    }

    return _dictionary[_attribute_vector->get(i)];
  }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override { throw std::logic_error("DictionaryColumn is immutable"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return std::make_shared<std::vector<T>>(_dictionary); }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary.at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto it = std::lower_bound(_dictionary.cbegin(), _dictionary.cend(), value);
    if (it == _dictionary.cend()) return INVALID_VALUE_ID;
    return std::distance(_dictionary.cbegin(), it);
  }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto it = std::upper_bound(_dictionary.cbegin(), _dictionary.cend(), value);
    if (it == _dictionary.cend()) return INVALID_VALUE_ID;
    return std::distance(_dictionary.cbegin(), it);
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary.size(); }

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
    T value = _dictionary.at(_attribute_vector->get(chunk_offset));
    buffer << value;
    uint32_t length = buffer.str().length();
    // writing byte representation of length
    buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

    // appending the new string to the already present string
    row_string += buffer.str();
  }

 protected:
  std::vector<T> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;

  static std::shared_ptr<BaseAttributeVector> _create_fitted_attribute_vector(size_t unique_values_count, size_t size) {
    if (unique_values_count <= std::numeric_limits<uint8_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint8_t>>(size);
    } else if (unique_values_count <= std::numeric_limits<uint16_t>::max()) {
      return std::make_shared<FittedAttributeVector<uint16_t>>(size);
    } else {
      return std::make_shared<FittedAttributeVector<uint32_t>>(size);
    }
  }
};
}  // namespace opossum
