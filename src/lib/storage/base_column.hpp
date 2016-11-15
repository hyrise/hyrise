#pragma once

#include <memory>

#include "column_visitable.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn
class BaseColumn {
 public:
  BaseColumn() = default;

  // copying a column is not allowed
  // copying whole columns is expensive
  BaseColumn(BaseColumn const &) = delete;
  BaseColumn &operator=(const BaseColumn &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseColumn(BaseColumn &&) = default;
  BaseColumn &operator=(BaseColumn &&) = default;

  // returns the value at a given position
  virtual const AllTypeVariant operator[](const size_t i) const = 0;

  // appends the value at the end of the column
  virtual void append(const AllTypeVariant &val) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // calls the column-specific handler in an operator (visitor pattern)
  virtual void visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) = 0;
};
}  // namespace opossum
