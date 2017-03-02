#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_column.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector
template <typename T>
class ValueColumn : public BaseColumn {
 public:
  ValueColumn() = default;

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return _values.at(i); }

  // add a value to the end
  void append(const AllTypeVariant& val) override { _values.push_back(type_cast<T>(val)); }

  // returns all values
  const tbb::concurrent_vector<T>& values() const { return _values; }
  tbb::concurrent_vector<T>& values() { return _values; }

  // return the number of entries
  size_t size() const override { return _values.size(); }

  // visitor pattern, see base_column.hpp
  void visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) override {
    visitable.handle_value_column(*this, std::move(context));
  }

  // writes the length and value at the chunk_offset to the end off row_string
  void write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const override {
    std::stringstream buffer;
    // buffering value at chunk_offset
    buffer << _values[chunk_offset];
    uint32_t length = buffer.str().length();
    // writing byte representation of length
    buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

    // appending the new string to the already present string
    row_string += buffer.str();
  }

 protected:
  tbb::concurrent_vector<T> _values;
};
}  // namespace opossum
