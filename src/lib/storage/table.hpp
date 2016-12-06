#pragma once

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "chunk.hpp"
#include "common.hpp"
#include "types.hpp"

namespace opossum {
// A table is partitioned horizontally into a number of chunks
class Table {
 public:
  // creates a table
  // the parameter specifies the maximum chunk size, i.e., partition size
  // default (0) is an unlimited size. A table holds always at least one chunk
  explicit Table(const size_t chunk_size = 0, const bool auto_compress = false);

  // copying a table is not allowed
  Table(Table const &) = delete;
  Table &operator=(const Table &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  // returns the number of columns
  size_t col_count() const;

  // returns the number of rows
  size_t row_count() const;

  // returns the number of chunks
  size_t chunk_count() const;

  // returns the chunk with the given id
  Chunk &get_chunk(ChunkID chunk_id);
  const Chunk &get_chunk(ChunkID chunk_id) const;

  // adds a chunk to the table
  void add_chunk(Chunk chunk);

  // returns the column name of the nth column
  const std::string &column_name(size_t column_id) const;

  // returns the column type of the nth column
  const std::string &column_type(size_t column_id) const;

  // returns the column with the given name
  size_t column_id_by_name(const std::string &column_name) const;

  // return the maximum chunk size
  size_t chunk_size() const;

  // adds a column to the end, i.e., right, of the table
  void add_column(const std::string &name, const std::string &type, bool create_value_column = true);

  // inserts a row at the end of the table
  void append(std::vector<AllTypeVariant> values);

  // returns the number of the chunk and the position in the chunk for a given row
  // TODO(md): this would be a nice place to use structured bindings once they are supported by the compilers
  inline std::pair<ChunkID, ChunkOffset> locate_row(RowID row) const { return {row.chunk_id, row.chunk_offset}; }

  // calculates the row id from a given chunk and the chunk offset
  inline RowID calculate_row_id(ChunkID chunk, ChunkOffset offset) const { return RowID{chunk, offset}; }

  // enforces dictionary compression on a certain chunk
  void compress_chunk(ChunkID chunk_id);

 protected:
  // 0 means that the chunk has an unlimited size.
  const size_t _chunk_size;
  const bool _auto_compress;
  std::vector<Chunk> _chunks;

  // these should be const strings, but having a vector of const values is a C++17 feature
  // that is not yet completely implemented in all compilers
  std::vector<std::string> _column_names;
  std::vector<std::string> _column_types;
};
}  // namespace opossum
