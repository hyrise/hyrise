#include "mvcc_data.hpp"

#include <shared_mutex>

#include "utils/assert.hpp"

namespace opossum {

MvccData::MvccData(const size_t size, CommitID begin_commit_id) {
  grow_by(size, INVALID_TRANSACTION_ID, begin_commit_id);
}

size_t MvccData::size() const { return _size; }

void MvccData::shrink() {
  // tbb::concurrent_vector::shrink_to_fit() is not thread-safe, we need a unique lock to it.
  //
  // https://software.intel.com/en-us/node/506205
  //   "Concurrent invocation of these operations on the same instance is not safe."
  // https://software.intel.com/en-us/node/506203
  //   "The method shrink_to_fit() merges several smaller arrays into a single contiguous array, which may improve
  //     access time."

  std::unique_lock<std::shared_mutex> lock{_mutex};
  tids.shrink_to_fit();
  begin_cids.shrink_to_fit();
  end_cids.shrink_to_fit();
}

void MvccData::grow_by(size_t delta, TransactionID transaction_id, CommitID begin_commit_id) {
  _size += delta;
  tids.grow_to_at_least(_size);

  for (auto chunk_offset = _size - delta; chunk_offset < _size; ++chunk_offset) {
    // We set the TIDs manually instead of passing them into grow_to_at_least, because this way we can do this
    // without synchronization. As the rows are not visible to anyone yet (MVCC vectors are resized before the table
    // is), there is no harm in using a relaxed model.
    tids[chunk_offset].store(transaction_id, std::memory_order_relaxed);
  }
  std::atomic_thread_fence(std::memory_order_seq_cst);

  begin_cids.grow_to_at_least(_size, begin_commit_id);
  end_cids.grow_to_at_least(_size, MAX_COMMIT_ID);
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

}  // namespace opossum
