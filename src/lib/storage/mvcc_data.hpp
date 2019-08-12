#pragma once

#include <atomic>
#include <shared_mutex>  // NOLINT lint thinks this is a C header or something

#include "types.hpp"
#include "utils/copyable_atomic.hpp"

namespace opossum {

/**
 * Stores visibility information for multiversion concurrency control
 */
struct MvccData {
  friend class Chunk;

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

  pmr_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
  pmr_vector<CommitID> begin_cids;                  ///< commit id when record was added
  pmr_vector<CommitID> end_cids;                    ///< commit id when record was deleted

  // Creates MVCC data that supports a maximum of `size` rows. If the underlying chunk has less rows, the extra rows
  // here are ignored. This is to avoid resizing the vectors, which would cause reallocations and require locking.
  explicit MvccData(const size_t size, CommitID begin_commit_id);

 private:
  /**
   * @brief Mutex used to manage access to MVCC data
   *
   * Exclusively locked in shrink()
   * Locked for shared ownership when MVCC data of a Chunk are accessed
   * via the get_scoped_mvcc_data_lock() getters
   */
  std::shared_mutex _mutex;  // TODO get rid of this
};

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

}  // namespace opossum
