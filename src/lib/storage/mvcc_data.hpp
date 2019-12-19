#pragma once

#include <atomic>
#include <shared_mutex>  // NOLINT lint thinks this is a C header or something

#include "types.hpp"
#include "utils/copyable_atomic.hpp"

namespace opossum {

/**
 * Stores visibility information for multiversion concurrency control.
 */
struct MvccData {
  friend class Chunk;

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

  // Note that these vectors may be longer than the chunk this MvccData struct belongs to (see the note in the
  // constructor)
  pmr_vector<copyable_atomic<TransactionID>> tids;  ///< 0 unless locked by a transaction
  pmr_vector<CommitID> begin_cids;                  ///< commit id when record was added
  pmr_vector<CommitID> end_cids;                    ///< commit id when record was deleted

  // This is used for optimizing the validation process. It is set during Chunk::finalize(). Consult
  // Validate::_on_execute for further details.
  std::optional<CommitID> max_begin_cid;

  // Creates MVCC data that supports a maximum of `size` rows. If the underlying chunk has less rows, the extra rows
  // here are ignored. This is to avoid resizing the vectors, which would cause reallocations and require locking.
  // For the same reason, we do not use reserve + resize, as we would have to rule out two transactions calling resize
  // concurrently.
  explicit MvccData(const size_t size, CommitID begin_commit_id);

  /**
   * The thread sanitizer (tsan) complains about concurrent writes and reads to begin/end_cids. That is because it is
   * unaware of their thread-safety being guaranteed by the update of the global last_cid. Furthermore, we exploit that
   * writes up to eight bytes are atomic on x64, which C++ and tsan do not know about. These helper methods were added
   * to .tsan-ignore.txt and can be used (carefully) to avoid those false positives.
   */
  CommitID get_begin_cid(const ChunkOffset offset) const;
  void set_begin_cid(const ChunkOffset offset, const CommitID commit_id);

  CommitID get_end_cid(const ChunkOffset offset) const;
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
};

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

}  // namespace opossum
