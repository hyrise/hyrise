#include "mvcc_data.hpp"

#include "utils/assert.hpp"

namespace opossum {

MvccData::MvccData(const size_t size, CommitID begin_commit_id) {
  DebugAssert(size > 0, "No point in having empty MVCC data, as it cannot grow");

  _begin_cids.resize(size, begin_commit_id);
  _end_cids.resize(size, MAX_COMMIT_ID);
  _tids.resize(size, copyable_atomic<TransactionID>{INVALID_TRANSACTION_ID});
  std::atomic_thread_fence(std::memory_order_seq_cst);
}

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data) {
  stream << "TIDs: ";
  for (const auto& tid : mvcc_data._tids) stream << tid.load() << ", ";
  stream << std::endl;

  stream << "BeginCIDs: ";
  for (const auto& begin_cid : mvcc_data._begin_cids) stream << begin_cid << ", ";
  stream << std::endl;

  stream << "EndCIDs: ";
  for (const auto& end_cid : mvcc_data._end_cids) stream << end_cid << ", ";
  stream << std::endl;

  return stream;
}

CommitID MvccData::get_begin_cid(const ChunkOffset offset) const {
  DebugAssert(offset < _begin_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _begin_cids[offset];
}

void MvccData::set_begin_cid(const ChunkOffset offset, const CommitID commit_id) {
  DebugAssert(offset < _begin_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  _begin_cids[offset] = commit_id;
}

CommitID MvccData::get_end_cid(const ChunkOffset offset) const {
  DebugAssert(offset < _end_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _end_cids[offset];
}

void MvccData::set_end_cid(const ChunkOffset offset, const CommitID commit_id) {
  DebugAssert(offset < _end_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  _end_cids[offset] = commit_id;
}

TransactionID MvccData::get_tid(const ChunkOffset offset) const {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _tids[offset];
}

void MvccData::set_tid(const ChunkOffset offset, const TransactionID new_transaction_id,
                       const std::memory_order memory_order) {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");

  _tids[offset].store(new_transaction_id, memory_order);
}

bool MvccData::compare_exchange_tid(const ChunkOffset offset, TransactionID expected_transaction_id,
                                    TransactionID new_transaction_id) {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");

  return _tids[offset].compare_exchange_strong(expected_transaction_id, new_transaction_id);
}

size_t MvccData::memory_usage() const {
  auto bytes = size_t{0};
  bytes += sizeof(_tids) + sizeof(_begin_cids) + sizeof(_end_cids);  // NOLINT
  bytes += _tids.size() * sizeof(decltype(_tids)::value_type);
  bytes += _begin_cids.size() * sizeof(decltype(_begin_cids)::value_type);
  bytes += _end_cids.size() * sizeof(decltype(_end_cids)::value_type);
  return bytes;
}

}  // namespace opossum
