#pragma once

#include <atomic>
#include <shared_mutex>  // NOLINT lint thinks this is a C header or something

#include "types.hpp"
#include "utils/copyable_atomic.hpp"

namespace opossum {

/**
 * Columns storing visibility information
 * for multiversion concurrency control
 */
struct MvccColumns {
  friend class Chunk;

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

  pmr_concurrent_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
  pmr_concurrent_vector<CommitID> begin_cids;                  ///< commit id when record was added
  pmr_concurrent_vector<CommitID> end_cids;                    ///< commit id when record was deleted

  explicit MvccColumns(const size_t size);

  size_t size() const;

  /**
   * Compacts the internal representation of
   * the mvcc columns in order to reduce fragmentation
   * Locks mvcc columns exclusively in order to do so
   */
  void shrink();

  /**
   * Grows all mvcc columns by the given delta
   *
   * @param begin_cid value all new begin_cids will be set to
   */
  void grow_by(size_t delta, CommitID begin_cid);

  void print(std::ostream& stream = std::cout) const;

 private:
  /**
   * @brief Mutex used to manage access to MVCC columns
   *
   * Exclusively locked in shrink()
   * Locked for shared ownership when MVCC columns of a Chunk are accessed
   * via the get_scoped_mvcc_columns_lock() getters
   */
  std::shared_mutex _mutex;

  size_t _size{0};
};

}  // namespace opossum
