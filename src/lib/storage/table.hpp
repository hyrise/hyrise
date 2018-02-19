#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"
#include "proxy_chunk.hpp"
#include "storage/index/index_info.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class TableStatistics;

// A table is partitioned horizontally into a number of chunks
class Table : private Noncopyable {
 public:
  // Creates a new table that has the same layout (column-{types, names}) as the input table
  static std::shared_ptr<Table> create_with_layout_from(const std::shared_ptr<const Table>& in_table,
                                                        const uint32_t max_chunk_size = Chunk::MAX_SIZE);

  /**
   * @returns whether both tables contain the same columns (in name and type) in the same order
   */
  static bool layouts_equal(const std::shared_ptr<const Table>& left, const std::shared_ptr<const Table>& right);

  // creates a table
  // the parameter specifies the maximum chunk size, i.e., partition size
  // default is the maximum allowed chunk size. A table holds always at least one chunk
  explicit Table(const uint32_t max_chunk_size = Chunk::MAX_SIZE);

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t column_count() const;

  // Returns the number of rows.
  // This number includes invalidated (deleted) rows.
  // Use approx_valid_row_count() for an approximate count of valid rows instead.
  uint64_t row_count() const;

  // returns the number of chunks (cannot exceed ChunkID (uint32_t))
  ChunkID chunk_count() const;

  // returns the chunk with the given id
  std::shared_ptr<Chunk> get_chunk(ChunkID chunk_id);
  std::shared_ptr<const Chunk> get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const;

  // Adds a chunk to the table. If the first chunk is empty, it is replaced.
  void emplace_chunk(const std::shared_ptr<Chunk>& chunk);

  // Returns a list of all column names.
  const std::vector<std::string>& column_names() const;

  // returns the column name of the nth column
  const std::string& column_name(ColumnID column_id) const;

  // returns the data type of the nth column
  DataType column_type(ColumnID column_id) const;

  // return whether nth column is nullable
  bool column_is_nullable(ColumnID column_id) const;

  // returns the vector of column types
  const std::vector<DataType>& column_types() const;

  // returns the vector of column nullables
  const std::vector<bool>& column_nullables() const;

  // Returns the column with the given name.
  // This method is intended for debugging purposes only.
  // It does not verify whether a column name is unambiguous.
  ColumnID column_id_by_name(const std::string& column_name) const;

  // return the maximum chunk size (cannot exceed ChunkOffset (uint32_t))
  uint32_t max_chunk_size() const;

  // adds column definition without creating the actual columns
  void add_column_definition(const std::string& name, DataType data_type, bool nullable = false);

  // adds a column to the end, i.e., right, of the table
  void add_column(const std::string& name, DataType data_type, bool nullable = false);

  // inserts a row at the end of the table
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

  // returns one materialized value
  // multiversion concurrency control values of chunks are ignored
  // - table needs to be validated before by Validate operator
  // If you want to write efficient operators, back off!
  template <typename T>
  T get_value(const ColumnID column_id, const size_t row_number) const {
    PerformanceWarning("get_value() used");

    Assert(column_id < column_count(), "column_id invalid");

    size_t row_counter = 0u;
    for (auto& chunk : _chunks) {
      size_t current_size = chunk->size();
      row_counter += current_size;
      if (row_counter > row_number) {
        return get<T>((*chunk->get_column(column_id))[row_number + current_size - row_counter]);
      }
    }
    Fail("Row does not exist.");
  }

  // creates a new chunk and appends it
  void create_new_chunk();

  std::unique_lock<std::mutex> acquire_append_mutex();

  void set_table_statistics(std::shared_ptr<TableStatistics> table_statistics) { _table_statistics = table_statistics; }

  std::shared_ptr<TableStatistics> table_statistics() { return _table_statistics; }
  std::shared_ptr<const TableStatistics> table_statistics() const { return _table_statistics; }

  /**
   * Determines whether this table consists solely of ReferenceColumns, in which case it is a TableType::References,
   * or contains Dictionary/ValueColumns, which makes it a TableType::Data.
   * A table containing both ReferenceColumns and Dictionary/ValueColumns is invalid.
   */
  TableType get_type() const;

  std::vector<IndexInfo> get_indexes() const;

  template <typename Index>
  void create_index(const std::vector<ColumnID>& column_ids, const std::string& name = "") {
    ColumnIndexType index_type = get_index_type_of<Index>();

    for (auto& chunk : _chunks) {
      chunk->create_index<Index>(column_ids);
    }
    IndexInfo i = {column_ids, name, index_type};
    _indexes.emplace_back(i);
  }

  /**
   * For debugging purposes, makes an estimation about the memory used by this Table (including Chunk and Columns)
   */
  size_t estimate_memory_usage() const;

 protected:
  const uint32_t _max_chunk_size;
  std::vector<std::shared_ptr<Chunk>> _chunks;

  // these should be const strings, but having a vector of const values is a C++17 feature
  // that is not yet completely implemented in all compilers
  std::vector<std::string> _column_names;
  std::vector<DataType> _column_types;
  std::vector<bool> _column_nullable;

  std::shared_ptr<TableStatistics> _table_statistics;

  std::unique_ptr<std::mutex> _append_mutex;

  std::vector<IndexInfo> _indexes;
};
}  // namespace opossum
