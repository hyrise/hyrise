#include "segment_access_statistics.hpp"

namespace opossum {

std::chrono::time_point<std::chrono::steady_clock> AtomicTimedAccessStrategy::start_time = std::chrono::steady_clock::now();
std::chrono::duration<double, std::milli> AtomicTimedAccessStrategy::interval = std::chrono::milliseconds{1000};

// -------------------------------------------------------------------------------------------------------------------
SegmentAccessType SegmentAccessTypeTools::iterator_access_pattern(const std::shared_ptr<const PosList>& positions) {
  const auto max_items_to_compare = std::min(positions->size(), 100ul);

  auto state = State::Unknown;
  for (auto i = 1ul; i < max_items_to_compare; ++i) {
    const int64_t diff = static_cast<int64_t>(positions->operator[](i).chunk_offset) -
                         static_cast<int64_t>(positions->operator[](i - 1).chunk_offset);

    auto input = Input::Negative;
    if (diff == 0) input = Input::Zero;
    else if (diff == 1) input = Input::One;
    else if (diff > 0) input = Input::Positive;
    else if (diff == -1) input = Input::NegativeOne;

    state = _transitions[static_cast<uint32_t>(state)][static_cast<uint32_t>(input)];
  }

  switch(state) {
    case State::Unknown: case State::SeqInc: case State::SeqDec: return SegmentAccessType::IteratorSeqAccess;
    case State::RndInc: case State::RndDec: return SegmentAccessType::IteratorIncreasingAccess;
    case State::Rnd: return SegmentAccessType::IteratorRandomAccess;
    case State::Count: Fail("Unknown state.");
  }

  Fail("This line should never be reached. It is required so the compiler won't complain.");
}

// -------------------------------------------------------------------------------------------------------------------
uint64_t AtomicAccessStrategy::count(SegmentAccessType type) const {
  return _count[static_cast<uint32_t>(type)];
}

void AtomicAccessStrategy::reset() {
  for (auto type = 0u, end = static_cast<uint32_t>(SegmentAccessType::Count); type < end; ++type) {
    _count[static_cast<uint32_t>(type)] = 0;
  }
}

void AtomicAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[static_cast<uint32_t>(type)] += count;
}

std::string AtomicAccessStrategy::header() {
  return SegmentAccessTypeTools::headers;
}

std::vector<std::string> AtomicAccessStrategy::to_string() const {
  std::string str;
  str.reserve(static_cast<uint32_t>(SegmentAccessType::Count) * 4);
  str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

  for (uint8_t type = 1; type < static_cast<uint32_t>(SegmentAccessType::Count); ++type) {
    str.append(",");
    str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
  }

  return std::vector<std::string>{str};
}

// -------------------------------------------------------------------------------------------------------------------
uint64_t NoAccessStrategy::count(SegmentAccessType type) const {
  return 0;
}

void NoAccessStrategy::reset() {
}

void NoAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
}

std::string NoAccessStrategy::header() {
  return SegmentAccessTypeTools::headers;
}

std::vector<std::string> NoAccessStrategy::to_string() const {
  return {""};
}

// -------------------------------------------------------------------------------------------------------------------
AtomicTimedAccessStrategy::AtomicTimedAccessStrategy() {
  _counters.reserve(5000);
}

uint64_t AtomicTimedAccessStrategy::count(SegmentAccessType type) const {
  Fail("Not supported for AtomicTimedAccessStrategy");
}

void AtomicTimedAccessStrategy::reset() {
  _counters.clear();
}

AtomicTimedAccessStrategy::Counter& AtomicTimedAccessStrategy::_counter() {
  const uint64_t timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count() /
                        interval.count();

  if (_counters.empty() || _counters.back().timestamp < timestamp) {
    _counters.emplace_back(timestamp);
  }

  return _counters.back();
}

void AtomicTimedAccessStrategy::increase(SegmentAccessType type, uint64_t count) {
  auto& counter = _counter();
  counter.counter[static_cast<uint32_t>(type)] += count;
}

std::string AtomicTimedAccessStrategy::header() {
  return "timestamp," + SegmentAccessTypeTools::headers;
}

std::vector<std::string> AtomicTimedAccessStrategy::to_string() const {
  std::string str;
  str.reserve(static_cast<uint32_t>(SegmentAccessType::Count) * 4);
  std::vector<std::string> counters;
  counters.reserve(_counters.size());

  for (const auto& counter : _counters) {
    str.append(std::to_string(counter.timestamp));
    for (uint8_t type = 0; type < counter.counter.size(); ++type) {
      str.append(",");
      str.append(std::to_string(counter.counter[type]));
    }
    counters.push_back(str);
    str.clear();
  }

  return counters;
}

// -------------------------------------------------------------------------------------------------------------------

uint64_t NonLockingStrategy::count(SegmentAccessType type) const {
  return _count[static_cast<uint32_t>(type)];
}

void NonLockingStrategy::reset() {
  _count.fill(0);
}

void NonLockingStrategy::increase(SegmentAccessType type, uint64_t count) {
  _count[static_cast<uint32_t>(type)] += count;
}

std::string NonLockingStrategy::header() {
  return SegmentAccessTypeTools::headers;
}

std::vector<std::string> NonLockingStrategy::to_string() const {
  std::string str;
  str.reserve(static_cast<uint32_t>(SegmentAccessType::Count) * 4);
  str.append(std::to_string(count(static_cast<SegmentAccessType>(0))));

  for (uint8_t type = 1; type < static_cast<uint32_t>(SegmentAccessType::Count); ++type) {
    str.append(",");
    str.append(std::to_string(count(static_cast<SegmentAccessType>(type))));
  }

  return std::vector<std::string>{str};
}
// -------------------------------------------------------------------------------------------------------------------

}  // namespace opossum