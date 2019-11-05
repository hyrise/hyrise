#include "segment_access_statistics.hpp"

namespace opossum {


uint64_t AtomicAccessStrategy::count(SegmentAccessType type) const {
  return _count[type];
}

void AtomicAccessStrategy::reset_all() {
  for (auto type = 0u, end = static_cast<uint32_t>(Count); type < end; ++type) {
    _count[type] = 0;
  }
}

void AtomicAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[type] += count;
}
// -------------------------------------------------------------------------------------------------------------------

uint64_t NonLockingStrategy::count(SegmentAccessType type) const {
  return _count[type];
}

void NonLockingStrategy::reset_all() {
  _count.fill(0);
}

void NonLockingStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[type] += count;
}
// -------------------------------------------------------------------------------------------------------------------








}  // namespace opossum