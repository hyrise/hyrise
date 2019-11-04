#include "segment_access_statistics.hpp"

namespace opossum {

const SegmentAccessStatistics no_statistics{};
bool SegmentAccessStatistics::use_locking = false;

SegmentAccessStatistics::SegmentAccessStatistics() : _atomic_count{}, _count{} {}

void SegmentAccessStatistics::increase(AccessType type) {
  if (use_locking) {
    ++_atomic_count[type];
  }
  else {
    ++_count[type];
  }
}

void SegmentAccessStatistics::reset_all() {
  if (use_locking) {
    for (auto type = 0u, end = static_cast<uint32_t>(Count); type < end; ++type) {
      _atomic_count[type] = 0;
    }
  }
  else {
    _count.fill(0);
  }

}

uint64_t SegmentAccessStatistics::count(AccessType type) {
  if (use_locking) return _atomic_count[type];
  else return _count[type];
}

std::string SegmentAccessStatistics::to_string() const {
  std::string str;
  str.reserve(AccessType::Count * 4);

  if (use_locking) {
    str.append(std::to_string(_atomic_count[0]));
    for (auto it = _atomic_count.cbegin() + 1, end_it = _atomic_count.cend() - 1; it < end_it; ++it) {
      str.append(",");
      str.append(std::to_string(*it));
    }
  }
  else {
    str.append(std::to_string(_count[0]));
    for (auto it = _count.cbegin() + 1, end_it = _count.cend() - 1; it < end_it; ++it) {
      str.append(",");
      str.append(std::to_string(*it));
    }
  }
  return str;
}

}  // namespace opossum