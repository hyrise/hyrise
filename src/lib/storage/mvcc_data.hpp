#pragma once

#include <shared_mutex>

#include "types.hpp" // NEEDEDINCLUDE
#include "utils/copyable_atomic.hpp" // NEEDEDINCLUDE

namespace opossum {

/**
 * Stores visibility information for multiversion concurrency control
 */
struct MvccData {
  friend class Chunk;

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

  pmr_concurrent_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
  pmr_concurrent_vector<CommitID> begin_cids;                  ///< commit id when record was added
  pmr_concurrent_vector<CommitID> end_cids;                    ///< commit id when record was deleted

  explicit MvccData(const size_t size);

  size_t size() const;

  /**
   * Compacts the internal representation of
   * the mvcc data in order to reduce fragmentation
   * Locks mvcc data exclusively in order to do so
   */
  void shrink();

  /**
   * Grows mvcc data by the given delta
   *
   * @param begin_cid value all new begin_cids will be set to
   */
  void grow_by(size_t delta, CommitID begin_cid);

  void print(std::ostream& stream = std::cout) const;

 private:
  /**
   * @brief Mutex used to manage access to MVCC data
   *
   * Exclusively locked in shrink()
   * Locked for shared ownership when MVCC data of a Chunk are accessed
   * via the get_scoped_mvcc_data_lock() getters
   */
  std::shared_mutex _mutex;

  size_t _size{0};
};

}  // namespace opossum
