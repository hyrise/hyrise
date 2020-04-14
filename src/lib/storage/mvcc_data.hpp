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
  friend std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

 public:
  // The last commit id is reserved for uncommitted changes
  static constexpr CommitID MAX_COMMIT_ID = std::numeric_limits<CommitID>::max() - 1;

  // This is used for optimizing the validation process. It is set during Chunk::finalize(). Consult
  // Validate::_on_execute for further details.
  std::optional<CommitID> max_begin_cid;

  // Creates MVCC data that supports a maximum of `size` rows. If the underlying chunk has less rows, the extra rows
  // here are ignored. This is to avoid resizing the vectors, which would cause reallocations and require locking.
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

  TransactionID get_tid(const ChunkOffset offset) const;
  void set_tid(const ChunkOffset offset, const TransactionID transaction_id,
               const std::memory_order memory_order = std::memory_order_seq_cst);
  bool compare_exchange_tid(const ChunkOffset offset, TransactionID expected_transaction_id,
                            TransactionID new_transaction_id);

  size_t memory_usage() const;

 private:
  // These vectors are pre-allocated. Do not resize them as someone might be reading them concurrently.
  pmr_vector<CommitID> _begin_cids;                  // < commit id when record was added
  pmr_vector<CommitID> _end_cids;                    // < commit id when record was deleted
  pmr_vector<copyable_atomic<TransactionID>> _tids;  // < 0 unless locked by a transaction
};

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data);

}  // namespace opossum
