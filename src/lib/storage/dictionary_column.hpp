#pragma once

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
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
  virtual const AllTypeVariant operator[](const size_t i) const { return _dictionary[_attribute_vector->get(i)]; }

  // dictionary columns are immutable
  virtual void append(const AllTypeVariant&) { throw std::logic_error("DictionaryColumn is immutable"); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return std::make_shared<std::vector<T>>(_dictionary); }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // returns the corresponding value_id for a value.
  ValueID get_value_id(T value) const {
    auto pos = std::lower_bound(_dictionary.cbegin(), _dictionary.cend(), value);
    if (pos == _dictionary.cend() || *pos != value) {
      return INVALID_VALUE_ID;
    }
    return std::distance(_dictionary.cbegin(), pos);
  }

  // returns the smallest and largest (output order: smallest, largest. input order: parameter order not specified)
  // ValueIDs that qualify for the including interval [smaller, larger]. If smaller is not part of the dictionary, the
  // function will return the ValueID of the first value which appears to be larger. If larger is not part of the
  // dictionary, the function will return the ValueID of the first value which appears to be smaller.
  std::pair<ValueID, ValueID> get_value_id_range(T value1, T value2) const {
    T smaller = std::min(value1, value2);
    T larger = std::max(value1, value2);
    auto pos_min = std::lower_bound(_dictionary.cbegin(), _dictionary.cend(), smaller);
    // We do not need to search in the complete dictionary. max cannot have a position < pos_min
    auto pos_max = std::upper_bound(pos_min, _dictionary.cend(), larger);

    return {std::distance(_dictionary.cbegin(), pos_min), std::distance(_dictionary.cbegin(), pos_max) - 1};
  }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary.size(); }

  // return the number of entries
  virtual size_t size() const { return _attribute_vector->size(); }

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
