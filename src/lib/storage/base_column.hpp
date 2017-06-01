#pragma once

#include <memory>
#include <string>

#include "all_type_variant.hpp"
#include "column_visitable.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn
class BaseColumn {
 public:
  BaseColumn() = default;
  virtual ~BaseColumn() = default;

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

  // checks if columns supports null values
  virtual bool can_be_null() const = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // calls the column-specific handler in an operator (visitor pattern)
  virtual void visit(ColumnVisitable &visitable, std::shared_ptr<ColumnVisitableContext> context = nullptr) = 0;

  // writes the length and value at the chunk_offset to the end off row_string
  virtual void write_string_representation(std::string &row_string, const ChunkOffset chunk_offset) const = 0;

  // copies one of its own values to a different ValueColumn - mainly used for materialization
  // we cannot always use the materialize method below because sort results might come from different BaseColumns
  virtual void copy_value_to_value_column(BaseColumn &value_column, ChunkOffset chunk_offset) const = 0;
};
}  // namespace opossum
