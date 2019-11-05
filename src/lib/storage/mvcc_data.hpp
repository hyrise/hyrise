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

  pmr_concurrent_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
  pmr_concurrent_vector<CommitID> begin_cids;                  ///< commit id when record was added
  pmr_concurrent_vector<CommitID> end_cids;                    ///< commit id when record was deleted

  // This is used for optimizing the validation process. It is set during Chunk::finalize(). Consult
  // Validate::_on_execute for further details.
  std::optional<CommitID> max_begin_cid;

  explicit MvccData(const size_t size, CommitID begin_commit_id);

  size_t size() const;

  /**
   * Compacts the internal representation of the mvcc data in order to reduce fragmentation.
   * Locks mvcc data exclusively in order to do so.
   */
  void shrink();

  /**
   * Grows mvcc data by the given delta
   */
  void grow_by(size_t delta, TransactionID transaction_id, CommitID begin_commit_id);

  /**
   * The thread sanitzer (tsan) complains about concurrent writes and reads to begin/end_cids. That is because it is
   * unaware of their thread-safety being guaranteed by the update of the global last_cid. Furthermore, we exploit that
   * writes up to eight bytes are atomic on x64, which C++ and tsan do not know about. These helper methods were added
   * to .tsan-ignore.txt and can be used (carefully) to avoid those false positives.
   */
  void set_begin_cid(const ChunkOffset offset, const CommitID commit_id);
  void set_end_cid(const ChunkOffset offset, const CommitID commit_id);

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

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

}  // namespace opossum
