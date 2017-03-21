#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "tbb/concurrent_vector.h"

#include "base_column.hpp"
#include "base_index.hpp"
#include "movable_atomic.hpp"
#include "value_column.hpp"

namespace opossum {

// A chunk is a horizontal partition of a table.
// It stores the data column by column.
class Chunk {
 public:
  static const CommitID MAX_COMMIT_ID;

  /**
   * Columns storing visibility information
   * for multiversion concurrency control
   */
  struct MvccColumns {
    tbb::concurrent_vector<movable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
    tbb::concurrent_vector<CommitID> begin_cids;                 ///< commit id when record was added
    tbb::concurrent_vector<CommitID> end_cids;                   ///< commit id when record was deleted
    mutable std::shared_mutex _mutex;
  };

 public:
  // creates an empty chunk without mvcc columns
  Chunk();
  explicit Chunk(const bool has_mvcc_columns);

  // copying a chunk is not allowed
  Chunk(const Chunk &) = delete;
  Chunk &operator=(const Chunk &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  Chunk(Chunk &&) = default;
  Chunk &operator=(Chunk &&) = default;

  // adds a column to the "right" of the chunk
  void add_column(std::shared_ptr<BaseColumn> column);

  // replaces the current column at column_id with the passed column
  void set_column(size_t column_id, std::shared_ptr<BaseColumn> column);

  // returns the number of columns
  size_t col_count() const;

  // returns the number of rows
  size_t size() const;

  // adds a new row, given as a list of values, to the chunk
  // note this is slow and not thread-safe and should be used for testing purposes only
  void append(std::vector<AllTypeVariant> values);

  // returns the column at a given position
  std::shared_ptr<BaseColumn> get_column(size_t column_id) const;

  bool has_mvcc_columns() const;

  MvccColumns &mvcc_columns();
  const MvccColumns &mvcc_columns() const;

  /**
   * Compacts the internal represantion of
   * the mvcc columns in order to reduce fragmentation
   * Note: not thread-safe
   */
  void shrink_mvcc_columns();

  /**
   * Grows all mvcc columns by the given delta
   *
   * @param begin_cid value all new begin_cids will be set to
   */
  void grow_mvcc_column_size_by(size_t delta, CommitID begin_cid);

  /**
   * Moves the mvcc columns from chunk to this instance
   * Used to transfer the mvcc columns to the new chunk after chunk compression
   * Note: not thread-safe
   */
  void move_mvcc_columns_from(Chunk &chunk);

  std::vector<std::shared_ptr<BaseIndex>> get_indices_for(
      const std::vector<std::shared_ptr<BaseColumn>> &columns) const;

  template <typename Index>
  std::shared_ptr<BaseIndex> create_index(std::shared_ptr<BaseColumn> index_column) {
    auto index = std::make_shared<Index>(std::vector<std::shared_ptr<BaseColumn>>({index_column}));
    _indices.emplace_back(index);
    return index;
  }

  bool references_only_one_table() const;

 protected:
  tbb::concurrent_vector<std::shared_ptr<BaseColumn>> _columns;
  std::unique_ptr<MvccColumns> _mvcc_columns;
  std::vector<std::shared_ptr<BaseIndex>> _indices;
};

}  // namespace opossum
