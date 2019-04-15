#include "mvcc_data.hpp"

#include <shared_mutex>

#include "concurrency/transaction_manager.hpp"
#include "utils/assert.hpp"

namespace opossum {

MvccData::MvccData(const size_t size) { grow_by(size); }

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

void MvccData::grow_by(size_t delta) {
  _size += delta;
  tids.grow_to_at_least(_size);
  begin_cids.grow_to_at_least(_size, MAX_COMMIT_ID);
  end_cids.grow_to_at_least(_size, MAX_COMMIT_ID);
}

void MvccData::print(std::ostream& stream) const {
  stream << "TIDs: ";
  for (const auto& tid : tids) stream << tid << ", ";
  stream << std::endl;

  stream << "BeginCIDs: ";
  for (const auto& begin_cid : begin_cids) stream << begin_cid << ", ";
  stream << std::endl;

  stream << "EndCIDs: ";
  for (const auto& end_cid : end_cids) stream << end_cid << ", ";
  stream << std::endl;
}

}  // namespace opossum
