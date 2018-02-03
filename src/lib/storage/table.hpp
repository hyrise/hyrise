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
#include "storage/table_layout.hpp"
#include "type_cast.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class TableStatistics;

// A table is partitioned horizontally into a number of chunks
class Table : private Noncopyable {
 public:
  // creates a table
  // the parameter specifies the maximum chunk size, i.e., partition size
  // default is the maximum allowed chunk size. A table holds always at least one chunk
  explicit Table(const TableLayout& layout, ChunkUseMvcc use_mvcc = ChunkUseMvcc::No, const uint32_t max_chunk_size = Chunk::MAX_SIZE);

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  const TableLayout& layout() const;

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

  std::shared_ptr<Chunk> emplace_chunk(const std::vector<std::shared_ptr<BaseColumn>>& columns,
                                       const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt,
                                       const std::shared_ptr<Chunk::AccessCounter>& access_counter = nullptr);

  // return the maximum chunk size (cannot exceed ChunkOffset (uint32_t))
  uint32_t max_chunk_size() const;

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

 protected:
  const TableLayout _layout;
  const ChunkUseMvcc _use_mvcc;
  const uint32_t _max_chunk_size;
  std::vector<std::shared_ptr<Chunk>> _chunks;
  std::shared_ptr<TableStatistics> _table_statistics;
  std::unique_ptr<std::mutex> _append_mutex;
  std::vector<IndexInfo> _indexes;
};
}  // namespace opossum
