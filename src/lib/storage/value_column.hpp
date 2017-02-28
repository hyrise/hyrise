#pragma once

#include <iostream>
#include <limits>
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

  // Create a ValueColumn with the given values
  explicit ValueColumn(std::vector<T>&& values) : _values(std::move(values)) {}

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    /*
    Handle null values, this is only used for testing the results of joins so far.
    In order to be able to define an expected output table, we need to replace INVALID_CHUNK_OFFSET
    with some printable character, in our case 0, resp. "0".
    Since there is no constructor for String, which takes a numeric 0, we have to differentiate between numbers and
    strings.

    This should be replaced as soon as we have proper NULL values in Opossum.
    Similar code is in dictionary_column.hpp
    */
    if (i == INVALID_CHUNK_OFFSET) {
      if (std::is_same<T, std::string>::value) {
        return "0";
      }
      return T(0);
    }
    return _values.at(i);
  }

  // add a value to the end
  void append(const AllTypeVariant& val) override;

  // returns all values
  const std::vector<T>& values() const { return _values; }
  std::vector<T>& values() { return _values; }

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
  std::vector<T> _values;
};

// generic implementation for append
template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  _values.push_back(type_cast<T>(val));
}

// specialized implementation for String ValueColumns
// includes a length check
template <>
inline void ValueColumn<std::string>::append(const AllTypeVariant& val) {
  auto typed_val = type_cast<std::string>(val);
  if (typed_val.length() > std::numeric_limits<StringLength>::max()) {
    throw std::runtime_error("String value is too long to append!");
  }
  _values.push_back(typed_val);
}

}  // namespace opossum
