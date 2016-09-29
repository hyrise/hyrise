#pragma once

#include <iostream>
#include <vector>

#include "base_column.hpp"

namespace opossum {

template <typename T>
class ValueColumn : public BaseColumn {
 public:
  // default constructor
  ValueColumn() {}

  // return the value at a certain position
  virtual AllTypeVariant operator[](const size_t i) const { return _values[i]; }

  // add a value to the end
  virtual void append(const AllTypeVariant& val) { _values.push_back(type_cast<T>(val)); }

  // returns all values
  const std::vector<T>& get_values() const { return _values; }

  // return the number of entries
  virtual size_t size() const { return _values.size(); }

 protected:
  std::vector<T> _values;
};
}  // namespace opossum
