#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_column.hpp"
#include "value_column.hpp"

namespace opossum {
// A chunk is a horizontal partition of a table.
// It stores the data column by column.
class Chunk {
 public:
  // creates an empty chunk
  Chunk() = default;

  // copying a chunk is not allowed
  Chunk(const Chunk &) = delete;
  Chunk &operator=(const Chunk &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk &&) = default;
  Chunk &operator=(Chunk &&) = default;

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // returns the number of columns
  size_t col_count() const;

  // returns the number of rows
  size_t size() const;

  // adds a new row, given as a list of values, to the chunk
  void append(std::vector<AllTypeVariant> values);

  // returns the column at a given position
  std::shared_ptr<BaseColumn> get_column(size_t column_id) const;

  // mvcc columns
  tbb::concurrent_vector<uint32_t> _TIDs;
  tbb::concurrent_vector<uint32_t> _begin_CIDs;
  tbb::concurrent_vector<uint32_t> _end_CIDs;

 protected:
  void set_mvcc_column_size(size_t new_size, uint32_t begin_CID);

 protected:
  std::vector<std::shared_ptr<BaseColumn>> _columns;
};
}  // namespace opossum
