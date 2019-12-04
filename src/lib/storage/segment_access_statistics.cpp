#include "segment_access_statistics.hpp"

namespace opossum {

std::chrono::time_point<std::chrono::steady_clock> AtomicTimedAccessStrategy::start_time = std::chrono::steady_clock::now();
std::chrono::duration<double, std::milli> AtomicTimedAccessStrategy::interval = std::chrono::milliseconds{1000};
// -------------------------------------------------------------------------------------------------------------------
uint64_t AtomicAccessStrategy::count(SegmentAccessType type) const {
  return _count[type];
}

void AtomicAccessStrategy::reset() {
  for (auto type = 0u, end = static_cast<uint32_t>(Count); type < end; ++type) {
    _count[type] = 0;
  }
}

void AtomicAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[type] += count;
}

std::string AtomicAccessStrategy::header() {
  return "Other,IteratorCreate,IteratorAccess,AccessorCreate,AcessorAccess,DictionaryAccess";
}

std::vector<std::string> AtomicAccessStrategy::to_string() const {
  std::string str;
  str.reserve(SegmentAccessType::Count * 4);
  str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

  for (uint8_t type = 1; type < Count; ++type) {
    str.append(",");
    str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
  }

  return std::vector<std::string>{str};
}

// -------------------------------------------------------------------------------------------------------------------
AtomicTimedAccessStrategy::AtomicTimedAccessStrategy() : _max_used_slot{0} {}

uint64_t AtomicTimedAccessStrategy::count(SegmentAccessType type) const {
  // not really supported
  return _count[_max_used_slot][type];
}

void AtomicTimedAccessStrategy::reset() {
  for (auto time_slot = 0u; time_slot < _time_slots; ++time_slot) {
    for (auto type = 0u, end = static_cast<uint32_t>(Count); type < end; ++type) {
      _count[time_slot][type] = 0;
    }
  }
  _max_used_slot = 0;
}

void AtomicTimedAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
  const auto time_slot = _time_slot();
  _count[time_slot][type] += count;
}

uint64_t AtomicTimedAccessStrategy::_time_slot() {
  const uint64_t slot = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() /
         interval.count();
  if (slot > _max_used_slot) {
    _max_used_slot = slot;
  }
  return slot;
}

std::string AtomicTimedAccessStrategy::header() {
  return "timestamp,Other,IteratorCreate,IteratorAccess,AccessorCreate,AcessorAccess,DictionaryAccess";
}

std::vector<std::string> AtomicTimedAccessStrategy::to_string() const {
  std::string str;
  str.reserve(SegmentAccessType::Count * 4);
  std::vector<std::string> counters;
  counters.reserve(_max_used_slot);

  for (auto time_slot = 0u; time_slot <= _max_used_slot; ++time_slot) {
    uint64_t sum = 0;
    for (uint8_t type = 0; type < Count; ++type) {
      sum += _count[time_slot][type];
    }
    if (sum > 0) {
      str.append(std::to_string(time_slot) + ',' + std::to_string(_count[time_slot][0]));
      for (uint8_t type = 1; type < Count; ++type) {
        str.append(",");
        str.append(std::to_string(_count[time_slot][type]));
      }
      counters.push_back(str);
      str.clear();
    }
  }

  if (counters.empty()) {
    str.append("0,0");
    for (uint8_t type = 1; type < Count; ++type) {
      str.append(",0");
    }
    counters.push_back(str);
  }

  return counters;
}

// -------------------------------------------------------------------------------------------------------------------

uint64_t NonLockingStrategy::count(SegmentAccessType type) const {
  return _count[type];
}

void NonLockingStrategy::reset() {
  _count.fill(0);
}

void NonLockingStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[type] += count;
}

std::string NonLockingStrategy::header() {
  return "Other,IteratorCreate,IteratorAccess,AccessorCreate,AcessorAccess,DictionaryAccess";
}

std::vector<std::string> NonLockingStrategy::to_string() const {
  std::string str;
  str.reserve(SegmentAccessType::Count * 4);
  str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

  for (uint8_t type = 1; type < Count; ++type) {
    str.append(",");
    str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
  }

  return std::vector<std::string>{str};
}
// -------------------------------------------------------------------------------------------------------------------

}  // namespace opossum