#include "segment_access_statistics.hpp"

#include <sstream>

#include "storage/table.hpp"

namespace opossum {

template<typename T>
std::string SegmentAccessCounter<T>::to_string() const {
  std::ostringstream stream;
  stream << other << ',' << iterator_create << ',' << iterator_seq_access << ',' << iterator_increasing_access << ','
         << iterator_random_access << ',' << accessor_create << ',' << accessor_access << ',' << dictionary_access;
  return stream.str();
}

template<typename T>
void SegmentAccessCounter<T>::reset() {
  other = 0;
  iterator_create = 0;
  iterator_seq_access = 0;
  iterator_increasing_access = 0;
  iterator_random_access = 0;
  accessor_create = 0;
  accessor_access = 0;
  dictionary_access = 0;
}

template<typename T>
T SegmentAccessCounter<T>::sum() const {
  return other + iterator_create + iterator_seq_access + iterator_increasing_access + iterator_random_access +
         accessor_create + accessor_access + dictionary_access;
}

// Explicit instantiation of SegmentAccessCounters used
template
class SegmentAccessCounter<uint64_t>;

template
class SegmentAccessCounter<std::atomic_uint64_t>;

SegmentAccessStatisticsTools::AccessPattern
SegmentAccessStatisticsTools::iterator_access_pattern(const std::shared_ptr<const PosList>& positions) {
  const auto max_items_to_compare = std::min(positions->size(), 100ul);

  auto access_pattern = AccessPattern::Unknown;
  for (auto i = 1ul; i < max_items_to_compare; ++i) {
    const int64_t diff = static_cast<int64_t>(positions->operator[](i).chunk_offset) -
                         static_cast<int64_t>(positions->operator[](i - 1).chunk_offset);

    auto input = Input::Negative;
    if (diff == 0) input = Input::Zero;
    else if (diff == 1) input = Input::One;
    else if (diff > 0) input = Input::Positive;
    else if (diff == -1) input = Input::NegativeOne;

    access_pattern = _transitions[static_cast<uint32_t>(access_pattern)][static_cast<uint32_t>(input)];
  }

  return access_pattern;
}

// Sets all counters of all segments in tables to zero.
void SegmentAccessStatisticsTools::reset_all(const std::map<std::string, std::shared_ptr<Table>>& tables) {
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        segment_ptr->access_statistics.reset();
      }
    }
  }
}

void SegmentAccessStatistics::on_iterator_create(uint64_t count) {
  ++_counter.iterator_create;
  _counter.iterator_seq_access += count;
}

/**
 * Increases the corresponding access counter [iterator_seq_access | iterator_increasing_access |
 * iterator_random_access] by positions->size(). The access counter is derived from the first N elements in positions
 * (see SegmentAccessStatisticsTools::iterator_access_pattern).
 */
void SegmentAccessStatistics::on_iterator_create(const std::shared_ptr<const PosList>& positions) {
  ++_counter.iterator_create;
  const auto access_pattern = SegmentAccessStatisticsTools::iterator_access_pattern(positions);
  switch (access_pattern) {
    case SegmentAccessStatisticsTools::AccessPattern::Unknown:
    case SegmentAccessStatisticsTools::AccessPattern::SeqInc:
    case SegmentAccessStatisticsTools::AccessPattern::SeqDec:
      _counter.iterator_seq_access += positions->size();
      break;
    case SegmentAccessStatisticsTools::AccessPattern::RndInc:
    case SegmentAccessStatisticsTools::AccessPattern::RndDec:
      _counter.iterator_increasing_access += positions->size();
      break;
    case SegmentAccessStatisticsTools::AccessPattern::Rnd:
      _counter.iterator_random_access += positions->size();
  }
}

// count is currently not in use.
void SegmentAccessStatistics::on_accessor_create(uint64_t count) {
  ++_counter.accessor_create;
}

// chunk_offset is currently not in use. It may be used in the future to derive the access pattern.
void SegmentAccessStatistics::on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
  _counter.accessor_access += count;
}

void SegmentAccessStatistics::on_dictionary_access(uint64_t count) {
  _counter.dictionary_access += count;
}

void SegmentAccessStatistics::on_other_access(uint64_t count) {
  _counter.other += count;
}

void SegmentAccessStatistics::reset() {
  _counter.reset();
}

// creates a copy of the segments access counter.
SegmentAccessCounter<uint64_t> SegmentAccessStatistics::counter() const {
  SegmentAccessCounter<uint64_t> counter;
  counter.iterator_random_access = _counter.iterator_random_access;
  counter.iterator_increasing_access = _counter.iterator_increasing_access;
  counter.iterator_seq_access = _counter.iterator_seq_access;
  counter.other = _counter.other;
  counter.dictionary_access = _counter.dictionary_access;
  counter.accessor_access = _counter.accessor_access;
  counter.accessor_create = _counter.accessor_create;
  counter.iterator_create = _counter.iterator_create;
  return counter;
}

}  // namespace opossum