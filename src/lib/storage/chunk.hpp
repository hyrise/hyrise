#pragma once

#include <memory>
#include <string>
#include <vector>

#include "base_column.hpp"
#include "value_column.hpp"

namespace opossum {
class Print;
// A chunk is a horizontal partition of a table.
// It stores the data column by column.
class Chunk {
 public:
  // creates an empty chunk
  Chunk() = default;

  // copying a chunk is not allowed
  Chunk(const Chunk &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk &&) = default;

  Chunk &operator=(const Chunk &) = default;

  // TODO(anyone) Do we need to remove the copy assignment op as well?

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // returns the number of columns
  size_t col_count() const;

  // returns the number of rows
  size_t size() const;

  // adds a new row, given as a list of values, to the chunk
  void append(std::initializer_list<AllTypeVariant> values) DEV_ONLY;

  // returns the column at a given position
  std::shared_ptr<BaseColumn> get_column(size_t column_id) const;

  friend class Print;

 protected:
  std::vector<std::shared_ptr<BaseColumn>> _columns;
};
}  // namespace opossum
