#include "mvcc_data.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <ostream>

#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/copyable_atomic.hpp"

namespace hyrise {

MvccData::MvccData(const size_t size, CommitID begin_commit_id) {
  DebugAssert(size > 0, "No point in having empty MVCC data, as it cannot grow");

  _begin_cids.resize(size, copyable_atomic<CommitID>{begin_commit_id});
  _end_cids.resize(size, copyable_atomic<CommitID>{MAX_COMMIT_ID});
  _tids.resize(size, copyable_atomic<TransactionID>{INVALID_TRANSACTION_ID});
}

std::ostream& operator<<(std::ostream& stream, const MvccData& mvcc_data) {
  stream << "TIDs: ";
  for (const auto& tid : mvcc_data._tids) {
    stream << tid.load() << ", ";
  }
  stream << '\n';

  stream << "BeginCIDs: ";
  for (const auto& begin_cid : mvcc_data._begin_cids) {
    stream << begin_cid << ", ";
  }
  stream << '\n';

  stream << "EndCIDs: ";
  for (const auto& end_cid : mvcc_data._end_cids) {
    stream << end_cid << ", ";
  }
  stream << '\n';

  return stream;
}

CommitID MvccData::get_begin_cid(const ChunkOffset offset) const {
  DebugAssert(offset < _begin_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _begin_cids[offset];
}

void MvccData::set_begin_cid(const ChunkOffset offset, const CommitID commit_id, const std::memory_order memory_order) {
  DebugAssert(offset < _begin_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  _begin_cids[offset] = commit_id;
  _begin_cids[offset].store(commit_id, memory_order);
}

CommitID MvccData::get_end_cid(const ChunkOffset offset) const {
  DebugAssert(offset < _end_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _end_cids[offset];
}

void MvccData::set_end_cid(const ChunkOffset offset, const CommitID commit_id, const std::memory_order memory_order) {
  DebugAssert(offset < _end_cids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  _end_cids[offset].store(commit_id, memory_order);
}

TransactionID MvccData::get_tid(const ChunkOffset offset) const {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  return _tids[offset];
}

void MvccData::set_tid(const ChunkOffset offset, const TransactionID transaction_id,
                       const std::memory_order memory_order) {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");
  _tids[offset].store(transaction_id, memory_order);
}

bool MvccData::compare_exchange_tid(const ChunkOffset offset, TransactionID expected_transaction_id,
                                    TransactionID transaction_id) {
  DebugAssert(offset < _tids.size(), "offset out of bounds; MvccData insufficently preallocated?");

  return _tids[offset].compare_exchange_strong(expected_transaction_id, transaction_id);
}

size_t MvccData::memory_usage() const {
  auto bytes = sizeof(*this);
  bytes += _tids.capacity() * sizeof(decltype(_tids)::value_type);
  bytes += _begin_cids.capacity() * sizeof(decltype(_begin_cids)::value_type);
  bytes += _end_cids.capacity() * sizeof(decltype(_end_cids)::value_type);
  return bytes;
}

void MvccData::register_insert() {
  ++_pending_inserts;
}

void MvccData::deregister_insert() {
  const auto remaining_inserts = _pending_inserts--;
  Assert(remaining_inserts > 0, "Cannot decrement active insert count when no active inserts are left.");
}

uint32_t MvccData::pending_inserts() const {
  return _pending_inserts.load();
}

}  // namespace hyrise
