#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "boost/variant.hpp"
#include "chunk.hpp"
#include "storage/index/index_statistics.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class TableStatistics;

/**
 * A Table is partitioned horizontally into a number of chunks.
 */
class Table : private Noncopyable {
  friend class StorageTableTest;

 public:
  static std::shared_ptr<Table> create_dummy_table(const TableColumnDefinitions& column_definitions);

  // We want a common interface for tables that contain data (TableType::Data) and tables that contain reference
  // segments (TableType::References). The attribute max_chunk_size is only used for data tables. If it is unset,
  // Chunk::DEFAULT_SIZE is used. It must not be set for reference tables.
  Table(const TableColumnDefinitions& column_definitions, const TableType type,
        const std::optional<uint32_t> max_chunk_size = std::nullopt, const UseMvcc use_mvcc = UseMvcc::No);

  Table(const TableColumnDefinitions& column_definitions, const TableType type,
        std::vector<std::shared_ptr<Chunk>>&& chunks, const UseMvcc use_mvcc = UseMvcc::No);

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

  // returns the chunk with the given id
  std::shared_ptr<Chunk> get_chunk(ChunkID chunk_id);
  std::shared_ptr<const Chunk> get_chunk(ChunkID chunk_id) const;

  /**
   * Removes the chunk with the given id.
   * Makes sure that the the chunk was fully invalidated by the logical delete before deleting it physically.
   */
  void remove_chunk(ChunkID chunk_id);

  /**
   * Creates a new Chunk from a set of segments and appends it to this table.
   * When implementing operators, prefer building the Chunks upfront and adding them to the output table on
   * construction of the Table. This avoids having to append repeatedly to the tbb::concurrent_vector storing the Chunks
   *
   * Asserts that the @param segments match with the TableType (only ReferenceSegments or only data containing segments)
   *
   * @param mvcc_data   Has to be passed in iff the Table is a data Table that uses MVCC
   */
  void append_chunk(const Segments& segments, std::shared_ptr<MvccData> mvcc_data = nullptr,
                    const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt);

  // Create and append a Chunk consisting of ValueSegments.
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
    const auto chunk_count = _chunks.size();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto chunk = std::atomic_load(&_chunks[chunk_id]);
      if (!chunk) continue;

      size_t current_size = chunk->size();
      row_counter += current_size;
      if (row_counter > row_number) {
        return boost::get<T>(
            (*chunk->get_segment(column_id))[static_cast<ChunkOffset>(row_number + current_size - row_counter)]);
      }
    }
    Fail("Row does not exist.");
  }

  // Materialize a single Tuple
  std::vector<AllTypeVariant> get_row(size_t row_idx) const;

  // Materialize the entire Table
  std::vector<std::vector<AllTypeVariant>> get_rows() const;
  /** @} */

  std::unique_lock<std::mutex> acquire_append_mutex();

  /**
   * Tables, typically those stored in the StorageManager, can be associated with statistics to perform Cardinality
   * estimation during optimization.
   * @{
   */
  std::shared_ptr<TableStatistics> table_statistics() const;

  void set_table_statistics(const std::shared_ptr<TableStatistics>& table_statistics);
  /** @} */

  std::vector<IndexStatistics> indexes_statistics() const;

  template <typename Index>
  void create_index(const std::vector<ColumnID>& column_ids, const std::string& name = "") {
    SegmentIndexType index_type = get_index_type_of<Index>();

    const auto chunk_count = _chunks.size();
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      auto chunk = std::atomic_load(&_chunks[chunk_id]);
      Assert(chunk, "Did not expect deleted chunk here.");  // see #1686

      chunk->create_index<Index>(column_ids);
    }
    IndexStatistics index_statistics = {column_ids, name, index_type};
    _indexes.emplace_back(index_statistics);
  }

  /**
   * For debugging purposes, makes an estimation about the memory used by this Table (including Chunk and Segments)
   */
  size_t estimate_memory_usage() const;

 protected:
  const TableColumnDefinitions _column_definitions;
  const TableType _type;
  const UseMvcc _use_mvcc;
  const uint32_t _max_chunk_size;

  /**
   * To prevent data races for TableType::Data tables, we must access _chunks atomically.
   * This is due to the existence of the MvccDeletePlugin, which might modify shared pointers from a separate thread.
   *
   * With C++20 we will get std::atomic<std::shared_ptr<T>>, which allows us to omit the std::atomic_load() and
   * std::atomic_store() function calls.
   */
  tbb::concurrent_vector<std::shared_ptr<Chunk>> _chunks;

  std::shared_ptr<TableStatistics> _table_statistics;
  std::unique_ptr<std::mutex> _append_mutex;
  std::vector<IndexStatistics> _indexes;
};
}  // namespace opossum
