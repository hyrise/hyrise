#pragma once

// the linter wants this to be above everything else
#include <shared_mutex>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "copyable_atomic.hpp"
#include "scoped_locking_ptr.hpp"

#include "all_type_variant.hpp"
#include "types.hpp"

namespace opossum {

class BaseIndex;
class BaseColumn;

// A chunk is a horizontal partition of a table.
// It stores the data column by column.
//
// Find more information about this in our wiki: https://github.com/hyrise/zweirise/wiki/chunk-concept
class Chunk : private Noncopyable {
 public:
  static const CommitID MAX_COMMIT_ID;

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

 public:
  // creates an empty chunk without mvcc columns
  Chunk();
  explicit Chunk(const bool has_mvcc_columns);
  explicit Chunk(const PolymorphicAllocator<Chunk> &alloc);
  explicit Chunk(const PolymorphicAllocator<Chunk> &alloc, const bool has_mvcc_columns);

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk &&) = default;
  Chunk &operator=(Chunk &&) = default;

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // Atomically replaces the current column at column_id with the passed column
  void replace_column(size_t column_id, std::shared_ptr<BaseColumn> column);

  // returns the number of columns (cannot exceed ColumnID (uint16_t))
  uint16_t col_count() const;

  // returns the number of rows (cannot exceed ChunkOffset (uint32_t))
  uint32_t size() const;

  // adds a new row, given as a list of values, to the chunk
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

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
  std::shared_ptr<BaseColumn> get_column(ColumnID column_id) const;

  bool has_mvcc_columns() const;

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
   * Compacts the internal represantion of
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

  /**
   * Reuse mvcc from other chunk
   */
  void use_mvcc_columns_from(const Chunk &chunk);

  std::vector<std::shared_ptr<BaseIndex>> get_indices_for(
      const std::vector<std::shared_ptr<BaseColumn>> &columns) const;

  template <typename Index>
  std::shared_ptr<BaseIndex> create_index(const std::vector<std::shared_ptr<BaseColumn>> &index_columns) {
    auto index = std::make_shared<Index>(index_columns);
    _indices.emplace_back(index);
    return index;
  }

  bool references_only_one_table() const;

 protected:
  PolymorphicAllocator<Chunk> _alloc;
  pmr_concurrent_vector<std::shared_ptr<BaseColumn>> _columns;
  std::unique_ptr<MvccColumns> _mvcc_columns;
  pmr_vector<std::shared_ptr<BaseIndex>> _indices;
};

}  // namespace opossum
