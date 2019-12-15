#include "mvcc_data.hpp"

#include <shared_mutex>

#include "utils/assert.hpp"

namespace opossum {

MvccData::MvccData(const size_t size, CommitID begin_commit_id) {
  DebugAssert(size > 0, "No point in having empty MVCC data, as it cannot grow");

  tids.resize(size, copyable_atomic<TransactionID>{INVALID_TRANSACTION_ID});
  begin_cids.resize(size, begin_commit_id);
  end_cids.resize(size, MAX_COMMIT_ID);
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data) {
  stream << "TIDs: ";
  for (const auto& tid : mvcc_data.tids) stream << tid << ", ";
  stream << std::endl;

  stream << "BeginCIDs: ";
  for (const auto& begin_cid : mvcc_data.begin_cids) stream << begin_cid << ", ";
  stream << std::endl;

  stream << "EndCIDs: ";
  for (const auto& end_cid : mvcc_data.end_cids) stream << end_cid << ", ";
  stream << std::endl;

  return stream;
}

CommitID MvccData::get_begin_cid(const ChunkOffset offset) const { return begin_cids[offset]; }
void MvccData::set_begin_cid(const ChunkOffset offset, const CommitID commit_id) { begin_cids[offset] = commit_id; }

CommitID MvccData::get_end_cid(const ChunkOffset offset) const { return end_cids[offset]; }
void MvccData::set_end_cid(const ChunkOffset offset, const CommitID commit_id) { end_cids[offset] = commit_id; }

}  // namespace opossum
