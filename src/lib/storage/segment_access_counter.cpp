#include "segment_access_counter.hpp"

#include <sstream>

namespace opossum {

SegmentAccessCounter::SegmentAccessCounter() {
  DebugAssert(static_cast<size_t>(AccessType::Count) == access_type_string_mapping.size(),
              "access_type_string_mapping should contain as many entries as there are access types.");
}

SegmentAccessCounter::SegmentAccessCounter(const SegmentAccessCounter& other) { _set_counters(other); }

SegmentAccessCounter& SegmentAccessCounter::operator=(const SegmentAccessCounter& other) {
  if (this == &other) {
    return *this;
  }
  _set_counters(other);
  return *this;
}

void SegmentAccessCounter::_set_counters(const SegmentAccessCounter& counter) {
  for (auto counter_index = 0ul, size = _counters.size(); counter_index < size; ++counter_index) {
    _counters[counter_index] = counter._counters[counter_index].load();
  }
}

SegmentAccessCounter::CounterType& SegmentAccessCounter::operator[](const AccessType type) {
  return _counters[static_cast<size_t>(type)];
}

const SegmentAccessCounter::CounterType& SegmentAccessCounter::operator[](const AccessType type) const {
  return _counters[static_cast<size_t>(type)];
}

std::string SegmentAccessCounter::to_string() const {
  std::string result = std::to_string(_counters[0]);
  result.reserve(static_cast<size_t>(AccessType::Count) * 19);
  for (auto access_type = 1u; access_type < static_cast<size_t>(AccessType::Count); ++access_type) {
    result.append(",");
    result.append(std::to_string(_counters[access_type]));
  }
  return result;
}

SegmentAccessCounter::AccessType SegmentAccessCounter::access_type(const AbstractPosList& positions) {
  const auto access_pattern = _access_pattern(positions);
  switch (access_pattern) {
    case SegmentAccessCounter::AccessPattern::Point:
      return AccessType::Point;
    case SegmentAccessCounter::AccessPattern::SequentiallyIncreasing:
    case SegmentAccessCounter::AccessPattern::SequentiallyDecreasing:
      return AccessType::Sequential;
    case SegmentAccessCounter::AccessPattern::MonotonicallyIncreasing:
    case SegmentAccessCounter::AccessPattern::MonotonicallyDecreasing:
      return AccessType::Monotonic;
    case SegmentAccessCounter::AccessPattern::Random:
      return AccessType::Random;
  }
  Fail("This code should never be reached.");
}

// Iterates over the first n (currently 100) elements in positions to determine the access pattern
// (see enum AccessPattern in header).
// The access pattern is computed by building a finite-state machine. The states are given by the enum AccessPatten.
// The alphabet is defined by the internal enum Input.
// The initial state is AccessPattern::Point. positions is iterated over from the beginning. For two adjacent
// elements (in positions) the difference is computed and mapped to an element of the enum Input.
// That input is used to transition from one state to the next. The predefined, two dimensional array, TRANSITIONS,
// acts as the transition function.
SegmentAccessCounter::AccessPattern SegmentAccessCounter::_access_pattern(const AbstractPosList& positions) {
  // There are five possible inputs
  enum class Input { Zero, One, Positive, NegativeOne, Negative };

  // clang-format off
  constexpr std::array<std::array<AccessPattern, 5 /*|Input|*/>, 6 /*|AccessPattern|*/> TRANSITIONS{{
    //                         Input:   Zero                                    One                                     Positive,                               NegativeOne                             Negative                                  // NOLINT
    /* FROM Point */                   {AccessPattern::Point,                   AccessPattern::SequentiallyIncreasing,  AccessPattern::MonotonicallyIncreasing, AccessPattern::SequentiallyDecreasing,  AccessPattern::MonotonicallyDecreasing},  // NOLINT
    /* FROM SequentiallyIncreasing */  {AccessPattern::SequentiallyIncreasing,  AccessPattern::SequentiallyIncreasing,  AccessPattern::MonotonicallyIncreasing, AccessPattern::Random,                  AccessPattern::Random},                   // NOLINT
    /* FROM MonotonicallyIncreasing */ {AccessPattern::MonotonicallyIncreasing, AccessPattern::MonotonicallyIncreasing, AccessPattern::MonotonicallyIncreasing, AccessPattern::Random,                  AccessPattern::Random},                   // NOLINT
    /* FROM SequentiallyDecreasing */  {AccessPattern::SequentiallyDecreasing,  AccessPattern::Random,                  AccessPattern::Random,                  AccessPattern::SequentiallyDecreasing,  AccessPattern::MonotonicallyDecreasing},  // NOLINT
    /* FROM MonotonicallyDecreasing */ {AccessPattern::MonotonicallyDecreasing, AccessPattern::Random,                  AccessPattern::Random,                  AccessPattern::MonotonicallyDecreasing, AccessPattern::MonotonicallyDecreasing},  // NOLINT
    /* FROM Random */                  {AccessPattern::Random,                  AccessPattern::Random,                  AccessPattern::Random,                  AccessPattern::Random,                  AccessPattern::Random}                    // NOLINT
  }};
  // clang-format on

  const auto max_items_to_compare = std::min(positions.size(), 100ul);

  auto access_pattern = AccessPattern::Point;
  for (auto i = 1ul; i < max_items_to_compare; ++i) {
    const int64_t diff =
        static_cast<int64_t>(positions[i].chunk_offset) - static_cast<int64_t>(positions[i - 1].chunk_offset);

    auto input = Input{};
    if (diff == 0) {
      input = Input::Zero;
    } else if (diff == 1) {
      input = Input::One;
    } else if (diff > 0) {
      input = Input::Positive;
    } else if (diff == -1) {
      input = Input::NegativeOne;
    } else {
      input = Input::Negative;
    }

    access_pattern = TRANSITIONS[static_cast<size_t>(access_pattern)][static_cast<size_t>(input)];

    if (access_pattern == AccessPattern::Random) {
      // There is no way that leads back from the Random state, so we can stop here
      break;
    }
  }

  return access_pattern;
}

bool SegmentAccessCounter::operator==(const SegmentAccessCounter& other) const {
  for (auto counter_index = 0ul, size = _counters.size(); counter_index < size; ++counter_index) {
    if (_counters[counter_index] != other._counters[counter_index]) return false;
  }
  return true;
}

bool SegmentAccessCounter::operator!=(const SegmentAccessCounter& other) const { return !(*this == other); }

}  // namespace opossum
