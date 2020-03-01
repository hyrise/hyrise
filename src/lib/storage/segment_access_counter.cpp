#include "segment_access_counter.hpp"

#include <numeric>
#include <sstream>

#include "boost/format.hpp"
#include "storage/table.hpp"

namespace opossum {

SegmentAccessCounter::SegmentAccessCounter() {
  reset();
}

SegmentAccessCounter::SegmentAccessCounter(const SegmentAccessCounter& counter) {
  _set_counters(counter);
}

SegmentAccessCounter& SegmentAccessCounter::operator=(const SegmentAccessCounter& counter) {
  _set_counters(counter);
  return *this;
}

void SegmentAccessCounter::_set_counters(const SegmentAccessCounter& counter) {
  for (auto counter_index = 0ul, size = _counters.size(); counter_index < size; ++counter_index) {
    _counters[counter_index] = counter._counters[counter_index].load();
  }
}

SegmentAccessCounter::CounterType& SegmentAccessCounter::get(const AccessType type) {
  return _counters[(size_t)type];
}

uint64_t SegmentAccessCounter::sum() const {
  return std::accumulate(_counters.cbegin(), _counters.cend() - 1, 0ul);
}

std::string SegmentAccessCounter::to_string() const {
  std::string result = std::to_string(_counters[0]);
  result.reserve((size_t)AccessType::Count * 19);
  for (auto access_type = 1u; access_type < (size_t)AccessType::Count; ++access_type) {
    result.append(",");
    result.append(std::to_string(_counters[access_type]));
  }
  return result;
}

// Computes the AccessType from the given positions list by determining the access pattern
SegmentAccessCounter::AccessType SegmentAccessCounter::access_type(const PosList& positions){
  const auto access_pattern = _access_pattern(positions);
  switch (access_pattern) {
    case SegmentAccessCounter::AccessPattern::Point:
      return AccessType::Point;
    case SegmentAccessCounter::AccessPattern::SequentiallyIncreasing:
    case SegmentAccessCounter::AccessPattern::SequentiallyDecreasing:
      return AccessType::Sequential;
    case SegmentAccessCounter::AccessPattern::RandomlyIncreasing:
    case SegmentAccessCounter::AccessPattern::RandomlyDecreasing:
      return AccessType::Increasing;
    case SegmentAccessCounter::AccessPattern::Random:
      return AccessType::Random;
  }
  Fail("This code should never be reached.");
}

void SegmentAccessCounter::reset() {
  for (auto& counter : _counters) counter = 0ul;
}

// Sets all counters of all segments in tables to zero.
void SegmentAccessCounter::reset(const std::map<std::string, std::shared_ptr<Table>>& tables) {
  for (const auto& [table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count()); column_id < count;
           ++column_id) {
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        segment_ptr->access_counter.reset();
      }
    }
  }
}

SegmentAccessCounter::AccessPattern SegmentAccessCounter::_access_pattern(const PosList& positions) {
  // There are five possible inputs
  enum class Input { Zero, One, Positive, NegativeOne, Negative };

  constexpr std::array<std::array<AccessPattern, 5 /*|Input|*/>, 6 /*|AccessPattern|*/> _transitions{
    {{AccessPattern::Point, AccessPattern::SequentiallyIncreasing, AccessPattern::SequentiallyIncreasing,
       AccessPattern::SequentiallyDecreasing, AccessPattern::SequentiallyDecreasing},
      {AccessPattern::SequentiallyIncreasing, AccessPattern::SequentiallyIncreasing, AccessPattern::RandomlyIncreasing,
        AccessPattern::Random, AccessPattern::Random},
      {AccessPattern::RandomlyIncreasing, AccessPattern::RandomlyIncreasing, AccessPattern::RandomlyIncreasing,
        AccessPattern::Random, AccessPattern::Random},
      {AccessPattern::SequentiallyDecreasing, AccessPattern::Random, AccessPattern::Random,
        AccessPattern::SequentiallyDecreasing, AccessPattern::RandomlyDecreasing},
      {AccessPattern::RandomlyDecreasing, AccessPattern::Random, AccessPattern::Random,
        AccessPattern::RandomlyDecreasing, AccessPattern::RandomlyDecreasing},
      {AccessPattern::Random, AccessPattern::Random, AccessPattern::Random, AccessPattern::Random,
        AccessPattern::Random}}};

  const auto max_items_to_compare = std::min(positions.size(), 100ul);

  auto access_pattern = AccessPattern::Point;
  for (auto i = 1ul; i < max_items_to_compare; ++i) {
    const int64_t diff = static_cast<int64_t>(positions[i].chunk_offset) -
                         static_cast<int64_t>(positions[i - 1].chunk_offset);

    auto input = Input::Negative;
    if (diff == 0)
      input = Input::Zero;
    else if (diff == 1)
      input = Input::One;
    else if (diff > 0)
      input = Input::Positive;
    else if (diff == -1)
      input = Input::NegativeOne;

    access_pattern = _transitions[(size_t)access_pattern][(size_t)input];
  }

  return access_pattern;
}

}  // namespace opossum
