#pragma once

#include "../common.hpp"
#include "../types.hpp"

namespace opossum {

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn
class BaseColumn {
 public:
  BaseColumn() = default;

  // copying a column is not allowed
  // copying whole columns is expensive
  BaseColumn(BaseColumn const &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseColumn(BaseColumn &&) = default;

  // returns the value at a given position
  virtual const AllTypeVariant operator[](const size_t i) DEV_ONLY const = 0;

  // appends the value at the end of the column
  virtual void append(const AllTypeVariant &val) DEV_ONLY = 0;

  // returns the number of values
  virtual size_t size() const = 0;
};
}  // namespace opossum
