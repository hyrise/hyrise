#pragma once

#include <iostream>
#include <vector>

#include "base_column.hpp"

namespace opossum {

template <typename T>
class ValueColumn : public BaseColumn {
 public:
  ValueColumn() {}

  virtual AllTypeVariant operator[](const size_t i) const { return _values[i]; }

  virtual void append(const AllTypeVariant& val) { _values.push_back(type_cast<T>(val)); }

  const std::vector<T>& get_values() const { return _values; }

  virtual size_t size() const { return _values.size(); }

 protected:
  std::vector<T> _values;
};
}  // namespace opossum
