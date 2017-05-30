#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "chunk.hpp"
#include "common.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {
// A table is partitioned horizontally into a number of chunks
class Table {
 public:
  // creates a table
  // the parameter specifies the maximum chunk size, i.e., partition size
  // default (0) is an unlimited size. A table holds always at least one chunk
  explicit Table(const size_t chunk_size = 0);

  // copying a table is not allowed
  Table(Table const &) = delete;
  Table &operator=(const Table &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t col_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  uint32_t row_count() const;

  // returns the number of chunks (cannot exceed ChunkID (uint32_t))
  uint32_t chunk_count() const;

  // returns the chunk with the given id
  Chunk &get_chunk(ChunkID chunk_id);
  const Chunk &get_chunk(ChunkID chunk_id) const;

  // adds a chunk to the table
  void add_chunk(Chunk chunk);

  // returns the column name of the nth column
  const std::string &column_name(ColumnID column_id) const;

  // returns the column type of the nth column
  const std::string &column_type(ColumnID column_id) const;

  // returns the vector of column types
  const std::vector<std::string> &column_types() const;

  // returns the column with the given name
  ColumnID column_id_by_name(const std::string &column_name) const;

  // return the maximum chunk size (cannot exceed ChunkOffset (uint32_t))
  uint32_t chunk_size() const;

  // adds a column to the end, i.e., right, of the table
  void add_column(const std::string &name, const std::string &type, bool create_value_column = true);

  // inserts a row at the end of the table
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

  // returns one materialized value
  template <typename T>
  const T &get_value(const ColumnID column_id = 0u, const size_t row_number = 0u) const {
    size_t row_counter = 0u;
    for (auto &chunk : _chunks) {
      size_t current_size = chunk.size();
      row_counter += current_size;
      if (row_counter > row_number) {
        return get<T>((*chunk.get_column(column_id))[row_number + current_size - row_counter]);
      }
    }
    throw std::runtime_error("Row does not exist.");
  }

  // creates a new chunk and appends it
  void create_new_chunk();

  // returns the number of the chunk and the position in the chunk for a given row
  // TODO(md): this would be a nice place to use structured bindings once they are supported by the compilers
  std::pair<ChunkID, ChunkOffset> locate_row(RowID row) const { return {row.chunk_id, row.chunk_offset}; }

  // calculates the row id from a given chunk and the chunk offset
  RowID calculate_row_id(ChunkID chunk, ChunkOffset offset) const { return RowID{chunk, offset}; }

  std::unique_lock<std::mutex> acquire_append_mutex();

 protected:
  // 0 means that the chunk has an unlimited size.
  const uint32_t _chunk_size;
  std::vector<Chunk> _chunks;

  // these should be const strings, but having a vector of const values is a C++17 feature
  // that is not yet completely implemented in all compilers
  std::vector<std::string> _column_names;
  std::vector<std::string> _column_types;

  std::unique_ptr<std::mutex> _append_mutex;
};
}  // namespace opossum
