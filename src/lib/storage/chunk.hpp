#pragma once

#include <tbb/concurrent_vector.h>
#include <boost/container/pmr/memory_resource.hpp>

// the linter wants this to be above everything else
#include <shared_mutex>

#include <algorithm>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "index/column_index_type.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/copyable_atomic.hpp"
#include "utils/scoped_locking_ptr.hpp"

namespace opossum {

class BaseIndex;
class BaseColumn;

enum class ChunkUseAccessCounter { Yes, No };

/**
 * A Chunk is a horizontal partition of a table.
 * It stores the table's data column by column.
 * Optionally, mostly applying to StoredTables, it may also hold a set of MvccColumns.
 *
 * Find more information about this in our wiki: https://github.com/hyrise/hyrise/wiki/chunk-concept
 */
class Chunk : private Noncopyable {
 public:
  static const CommitID MAX_COMMIT_ID;
  static const ChunkOffset MAX_SIZE;

  /**
   * Columns storing visibility information
   * for multiversion concurrency control
   */
  struct MvccColumns {
    friend class Chunk;

   public:
    pmr_concurrent_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
    pmr_concurrent_vector<CommitID> begin_cids;                  ///< commit id when record was added
    pmr_concurrent_vector<CommitID> end_cids;                    ///< commit id when record was deleted

   private:
    /**
     * @brief Mutex used to manage access to MVCC columns
     *
     * Exclusively locked in shrink_to_fit()
     * Locked for shared ownership when MVCC columns are accessed
     * via the mvcc_columns() getters
     */
    std::shared_mutex _mutex;
  };

  /**
   * Data structure for storing chunk access times
   *
   * The chunk access times are tracked using ProxyChunk objects
   * that measure the cycles they were in scope using the RDTSC instructions.
   * The access times are added to a counter. The ChunkMetricCollection tasks
   * is regularly scheduled by the NUMAPlacementManager. This tasks takes a snapshot
   * of the current counter values and places them in a history. The history is
   * stored in a ring buffer, so that only a limited number of history items are
   * preserved.
   */
  struct AccessCounter {
    friend class Chunk;

   public:
    explicit AccessCounter(const PolymorphicAllocator<uint64_t>& alloc) : _history(_capacity, alloc) {}

    void increment() { _counter++; }
    void increment(uint64_t value) { _counter.fetch_add(value); }

    // Takes a snapshot of the current counter and adds it to the history
    void process() { _history.push_back(_counter); }

    // Returns the access time of the chunk during the specified number of
    // recent history sample iterations.
    uint64_t history_sample(size_t lookback) const;

    uint64_t counter() const { return _counter; }

   private:
    const size_t _capacity = 100;
    std::atomic<std::uint64_t> _counter{0};
    pmr_ring_buffer<uint64_t> _history;
  };

 public:
  explicit Chunk(UseMvcc mvcc_mode = UseMvcc::No, ChunkUseAccessCounter = ChunkUseAccessCounter::No);
  // If you're passing in an access_counter, this means that it is a derivative of an already existing chunk.
  // As such, it cannot have MVCC information.
  Chunk(const PolymorphicAllocator<Chunk>& alloc, const std::shared_ptr<AccessCounter> access_counter);
  Chunk(const PolymorphicAllocator<Chunk>& alloc, const UseMvcc mvcc_mode, const ChunkUseAccessCounter counter_mode);

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk&&) = default;
  Chunk& operator=(Chunk&&) = default;

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // Atomically replaces the current column at column_id with the passed column
  void replace_column(size_t column_id, std::shared_ptr<BaseColumn> column);

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
  std::shared_ptr<BaseColumn> get_mutable_column(ColumnID column_id) const;
  std::shared_ptr<const BaseColumn> get_column(ColumnID column_id) const;

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

  /**
   * Compacts the internal representation of
   * the mvcc columns in order to reduce fragmentation
   * Locks mvcc columns exclusively in order to do so
   */
  void shrink_mvcc_columns();

  /**
   * Grows all mvcc columns by the given delta
   *
   * @param begin_cid value all new begin_cids will be set to
   */
  void grow_mvcc_column_size_by(size_t delta, CommitID begin_cid);

  std::vector<std::shared_ptr<BaseIndex>> get_indices(
      const std::vector<std::shared_ptr<const BaseColumn>>& columns) const;
  std::vector<std::shared_ptr<BaseIndex>> get_indices(const std::vector<ColumnID> column_ids) const;

  std::shared_ptr<BaseIndex> get_index(const ColumnIndexType index_type,
                                       const std::vector<std::shared_ptr<const BaseColumn>>& columns) const;
  std::shared_ptr<BaseIndex> get_index(const ColumnIndexType index_type, const std::vector<ColumnID> column_ids) const;

  template <typename Index>
  std::shared_ptr<BaseIndex> create_index(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns) {
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
  std::shared_ptr<BaseIndex> create_index(const std::vector<ColumnID>& column_ids) {
    const auto columns = get_columns_for_ids(column_ids);
    return create_index<Index>(columns);
  }

  void remove_index(std::shared_ptr<BaseIndex> index);

  void migrate(boost::container::pmr::memory_resource* memory_source);

  std::shared_ptr<AccessCounter> access_counter() const { return _access_counter; }

  bool references_exactly_one_table() const;

  const PolymorphicAllocator<Chunk>& get_allocator() const;

  /**
   * For debugging purposes, makes an estimation about the memory used by this Chunk and its Columns
   */
  size_t estimate_memory_usage() const;

 private:
  std::vector<std::shared_ptr<const BaseColumn>> get_columns_for_ids(const std::vector<ColumnID>& column_ids) const;

 private:
  PolymorphicAllocator<Chunk> _alloc;
  pmr_concurrent_vector<std::shared_ptr<BaseColumn>> _columns;
  std::shared_ptr<MvccColumns> _mvcc_columns;
  std::shared_ptr<AccessCounter> _access_counter;
  pmr_vector<std::shared_ptr<BaseIndex>> _indices;
};

}  // namespace opossum
