#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "common.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class TableStatistics;

// A table is partitioned horizontally into a number of chunks
class Table : private Noncopyable {
 public:
  // creates a table
  // the parameter specifies the maximum chunk size, i.e., partition size
  // default (0) is an unlimited size. A table holds always at least one chunk
  explicit Table(const uint32_t chunk_size = 0);

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Table(Table &&) = default;
  Table &operator=(Table &&) = default;

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t col_count() const;

  // Returns the number of rows.
  // This number includes invalidated (deleted) rows.
  // Use approx_valid_row_count() for an approximate count of valid rows instead.
  uint64_t row_count() const;

  // Returns the number of valid rows (using approximate count of deleted rows)
  uint64_t approx_valid_row_count() const;

  // Increases the (approximate) count of invalid rows in the table (caused by deletes).
  void inc_invalid_row_count(uint64_t count);

  // returns the number of chunks (cannot exceed ChunkID (uint32_t))
  ChunkID chunk_count() const;

  // returns the chunk with the given id
  Chunk &get_chunk(ChunkID chunk_id);
  const Chunk &get_chunk(ChunkID chunk_id) const;

  // adds a chunk to the table
  void add_chunk(Chunk chunk);

  // Returns a list of all column names.
  const std::vector<std::string> &column_names() const;

  // returns the column name of the nth column
  const std::string &column_name(ColumnID column_id) const;

  // returns the column type of the nth column
  const std::string &column_type(ColumnID column_id) const;

  // return whether nth column is nullable
  bool column_is_nullable(ColumnID column_id) const;

  // returns the vector of column types
  const std::vector<std::string> &column_types() const;

  // returns the vector of column nullables
  const std::vector<bool> &column_nullables() const;

  // Returns the column with the given name.
  // This method is intended for debugging purposes only.
  // It does not verify whether a column name is unambiguous.
  ColumnID column_id_by_name(const std::string &column_name) const;

  // return the maximum chunk size (cannot exceed ChunkOffset (uint32_t))
  uint32_t chunk_size() const;

  // adds column definition without creating the actual columns
  void add_column_definition(const std::string &name, const std::string &type, bool nullable = false);

  // adds a column to the end, i.e., right, of the table
  void add_column(const std::string &name, const std::string &type, bool nullable = false);

  // inserts a row at the end of the table
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

  // returns one materialized value
  // multiversion concurrency control values of chunks are ignored
  // - table needs to be validated before by Validate operator
  // If you want to write efficient operators, back off!
  template <typename T>
  T get_value(const ColumnID column_id, const size_t row_number) const {
    size_t row_counter = 0u;
    for (auto &chunk : _chunks) {
      size_t current_size = chunk.size();
      row_counter += current_size;
      if (row_counter > row_number) {
        return get<T>((*chunk.get_column(column_id))[row_number + current_size - row_counter]);
      }
    }
    Fail("Row does not exist.");
    return {};
  }

  // creates a new chunk and appends it
  void create_new_chunk();

  // returns the number of the chunk and the position in the chunk for a given row
  // TODO(md): this would be a nice place to use structured bindings once they are supported by the compilers
  std::pair<ChunkID, ChunkOffset> locate_row(RowID row) const { return {row.chunk_id, row.chunk_offset}; }

  // calculates the row id from a given chunk and the chunk offset
  RowID calculate_row_id(ChunkID chunk, ChunkOffset offset) const { return RowID{chunk, offset}; }

  std::unique_lock<std::mutex> acquire_append_mutex();

  void set_table_statistics(std::shared_ptr<TableStatistics> table_statistics) { _table_statistics = table_statistics; }

  std::shared_ptr<TableStatistics> table_statistics() { return _table_statistics; }

 protected:
  // 0 means that the chunk has an unlimited size.
  const uint32_t _chunk_size;
  std::vector<Chunk> _chunks;

  // Stores the number of invalid (deleted) rows.
  // This is currently not an atomic due to performance considerations.
  // It is simply used as an estimate for the optimizer, and therefore does not need to be exact.
  uint64_t _approx_invalid_row_count{0};

  // these should be const strings, but having a vector of const values is a C++17 feature
  // that is not yet completely implemented in all compilers
  std::vector<std::string> _column_names;
  std::vector<std::string> _column_types;
  std::vector<bool> _column_nullable;

  std::shared_ptr<TableStatistics> _table_statistics;

  std::unique_ptr<std::mutex> _append_mutex;
};
}  // namespace opossum
