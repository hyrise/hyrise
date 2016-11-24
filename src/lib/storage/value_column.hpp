#pragma once

#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "base_column.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector
template <typename T>
class ValueColumn : public BaseColumn {
 public:
  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return _values.at(i); }

  // add a value to the end
  void append(const AllTypeVariant& val) override { _values.push_back(type_cast<T>(val)); }

  // returns all values
  const std::vector<T>& values() const { return _values; }

  // return the number of entries
  size_t size() const override { return _values.size(); }

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override {
    visitable.handle_value_column(*this, std::move(context));
  }

 protected:
  std::vector<T> _values;
};
}  // namespace opossum
