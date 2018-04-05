#pragma once

#include <tbb/concurrent_vector.h>
#include <boost/container/pmr/memory_resource.hpp>

// the linter wants this to be above everything else
#include <shared_mutex>

#include <algorithm>
#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "index/column_index_type.hpp"

#include "all_type_variant.hpp"
#include "chunk_access_counter.hpp"
#include "mvcc_columns.hpp"
#include "table_column_definition.hpp"
#include "types.hpp"
#include "utils/copyable_atomic.hpp"
#include "utils/scoped_locking_ptr.hpp"
#include "utils/create_ptr_aliases.hpp"

namespace opossum {

class BaseIndex;
class BaseColumn;
class ChunkStatistics;

using ChunkColumns = pmr_vector<BaseColumnSPtr>;

/**
 * A Chunk is a horizontal partition of a table.
 * It stores the table's data column by column.
 * Optionally, mostly applying to StoredTables, it may also hold a set of MvccColumns.
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/chunk-concept
 */
class Chunk : private Noncopyable {
 public:
  static const ChunkOffset MAX_SIZE;

  Chunk(const ChunkColumns& columns, MvccColumnsSPtr mvcc_columns = nullptr,
        const std::optional<PolymorphicAllocator<Chunk>>& alloc = std::nullopt,
        const ChunkAccessCounterSPtr access_counter = nullptr);

  // returns whether new rows can be appended to this Chunk
  bool is_mutable() const;

  // Atomically replaces the current column at column_id with the passed column
  void replace_column(size_t column_id, BaseColumnSPtr column);

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t column_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  uint32_t size() const;

  // adds a new row, given as a list of values, to the chunk
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(const std::vector<AllTypeVariant>& values);

  /**
   * Atomically accesses and returns the column at a given position
   *
   * Note: Concurrently with the execution of operators,
   *       ValueColumns might be exchanged with DictionaryColumns.
   *       Therefore, if you hold a pointer to a column, you can
   *       continue to use it without any inconsistencies.
   *       However, if you call get_column again, be aware that
   *       the return type might have changed.
   */
  BaseColumnSPtr get_mutable_column(ColumnID column_id) const;
  BaseColumnCSPtr get_column(ColumnID column_id) const;

  const ChunkColumns& columns() const;

  bool has_mvcc_columns() const;
  bool has_access_counter() const;

  /**
   * The locking pointer locks the columns non-exclusively
   * and unlocks them on destruction
   *
   * For improved performance, it is best to call this function
   * once and retain the reference as long as needed.
   *
   * @return a locking ptr to the mvcc columns
   */
  SharedScopedLockingPtr<MvccColumns> mvcc_columns();
  SharedScopedLockingPtr<const MvccColumns> mvcc_columns() const;

  std::vector<BaseIndexSPtr> get_indices(
      const std::vector<BaseColumnCSPtr>& columns) const;
  std::vector<BaseIndexSPtr> get_indices(const std::vector<ColumnID> column_ids) const;

  BaseIndexSPtr get_index(const ColumnIndexType index_type,
                                       const std::vector<BaseColumnCSPtr>& columns) const;
  BaseIndexSPtr get_index(const ColumnIndexType index_type, const std::vector<ColumnID> column_ids) const;

  template <typename Index>
  BaseIndexSPtr create_index(const std::vector<BaseColumnCSPtr>& index_columns) {
    DebugAssert(([&]() {
                  for (auto column : index_columns) {
                    const auto column_it = std::find(_columns.cbegin(), _columns.cend(), column);
                    if (column_it == _columns.cend()) return false;
                  }
                  return true;
                }()),
                "All columns must be part of the chunk.");

    auto index = std::make_shared<Index>(index_columns);
    _indices.emplace_back(index);
    return index;
  }

  template <typename Index>
  BaseIndexSPtr create_index(const std::vector<ColumnID>& column_ids) {
    const auto columns = get_columns_for_ids(column_ids);
    return create_index<Index>(columns);
  }

  void remove_index(BaseIndexSPtr index);

  void migrate(boost::container::pmr::memory_resource* memory_source);

  ChunkAccessCounterSPtr access_counter() const { return _access_counter; }

  bool references_exactly_one_table() const;

  const PolymorphicAllocator<Chunk>& get_allocator() const;

  ChunkStatisticsSPtr statistics() const;

  void set_statistics(ChunkStatisticsSPtr statistics);

  /**
   * For debugging purposes, makes an estimation about the memory used by this Chunk and its Columns
   */
  size_t estimate_memory_usage() const;

 private:
  std::vector<BaseColumnCSPtr> get_columns_for_ids(const std::vector<ColumnID>& column_ids) const;

 private:
  PolymorphicAllocator<Chunk> _alloc;
  ChunkColumns _columns;
  MvccColumnsSPtr _mvcc_columns;
  ChunkAccessCounterSPtr _access_counter;
  pmr_vector<BaseIndexSPtr> _indices;
  ChunkStatisticsSPtr _statistics;
};



}  // namespace opossum
