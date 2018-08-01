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
#include "storage/table_column_definition.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class TableStatistics;

/**
 * A Table is partitioned horizontally into a number of chunks.
 */
class Table : private Noncopyable {
 public:
  static std::shared_ptr<Table> create_dummy_table(const TableColumnDefinitions& column_definitions);

  explicit Table(const TableColumnDefinitions& column_definitions, const TableType type,
                 const uint32_t max_chunk_size = Chunk::MAX_SIZE, const UseMvcc use_mvcc = UseMvcc::No);
  /**
   * @defgroup Getter and convenience functions for the column definitions
   * @{
   */

  const TableColumnDefinitions& column_definitions() const;

  size_t column_count() const;

  const std::string& column_name(const ColumnID column_id) const;
  std::vector<std::string> column_names() const;

  DataType column_data_type(const ColumnID column_id) const;
  std::vector<DataType> column_data_types() const;

  bool column_is_nullable(const ColumnID column_id) const;
  std::vector<bool> columns_are_nullable() const;

  // Fail()s, if there is no column of that name
  ColumnID column_id_by_name(const std::string& column_name) const;

  /** @} */

  TableType type() const;

  UseMvcc has_mvcc() const;

  // return the maximum chunk size (cannot exceed ChunkOffset (uint32_t))
  uint32_t max_chunk_size() const;

  // Returns the number of rows.
  // This number includes invalidated (deleted) rows.
  // Use approx_valid_row_count() for an approximate count of valid rows instead.
  uint64_t row_count() const;

  /**
   * @return row_count() == 0
   */
  bool empty() const;

  /**
   * @defgroup Accessing and adding Chunks
   * @{
   */
  // returns the number of chunks (cannot exceed ChunkID (uint32_t))
  ChunkID chunk_count() const;

  // Returns all Chunks
  const std::vector<std::shared_ptr<Chunk>>& chunks() const;

  // returns the chunk with the given id
  std::shared_ptr<Chunk> get_chunk(ChunkID chunk_id);
  std::shared_ptr<const Chunk> get_chunk(ChunkID chunk_id) const;
  ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id);
  const ProxyChunk get_chunk_with_access_counting(ChunkID chunk_id) const;

  /**
   * Creates a new Chunk and appends it to this table.
   * Makes sure the @param columns match with the TableType (only ReferenceColumns or only data containing columns)
   * En/Disables MVCC for the Chunk depending on whether MVCC is enabled for the table (has_mvcc())
   * This is a convenience method to enable automatically creating a chunk with correct settings given a set of columns.
   * @param alloc
   */
  void append_chunk(const ChunkColumns& columns, const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt,
                    const std::shared_ptr<ChunkAccessCounter>& access_counter = nullptr);

  /**
   * Appends an existing chunk to this table.
   * Makes sure the columns in the chunk match with the TableType and the MVCC setting is the same as for the table.
   */
  void append_chunk(const std::shared_ptr<Chunk>& chunk);

  // Create and append a Chunk consisting of ValueColumns.
  void append_mutable_chunk();

  /** @} */

  /**
   * @defgroup Convenience methods for accessing/adding Table data. Slow, use only for testing!
   * @{
   */

  // inserts a row at the end of the table
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(const std::vector<AllTypeVariant>& values);

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

  /** @} */

  std::unique_lock<std::mutex> acquire_append_mutex();

  void set_table_statistics(std::shared_ptr<TableStatistics> table_statistics) { _table_statistics = table_statistics; }

  std::shared_ptr<TableStatistics> table_statistics() { return _table_statistics; }
  std::shared_ptr<const TableStatistics> table_statistics() const { return _table_statistics; }

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
  const TableColumnDefinitions _column_definitions;
  const TableType _type;
  const UseMvcc _use_mvcc;
  const uint32_t _max_chunk_size;
  std::vector<std::shared_ptr<Chunk>> _chunks;
  std::shared_ptr<TableStatistics> _table_statistics;
  std::unique_ptr<std::mutex> _append_mutex;
  std::vector<IndexInfo> _indexes;
};
}  // namespace opossum
