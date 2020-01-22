#include "segment_access_counter.hpp"

#include <sstream>

#include "boost/format.hpp"
#include "storage/table.hpp"

namespace opossum {

template<typename T>
SegmentAccessCounter::Counter<T>::Counter()
  : other{0}, iterator_create{0}, iterator_seq_access{0}, iterator_increasing_access{0},
    iterator_random_access{0}, accessor_create{0}, accessor_access{0}, dictionary_access{0} {
}

template<typename T>
std::string SegmentAccessCounter::Counter<T>::to_string() const {
//  std::ostringstream stream;
//  stream << other << ',' << iterator_create << ',' << iterator_seq_access << ',' << iterator_increasing_access << ','
//         << iterator_random_access << ',' << accessor_create << ',' << accessor_access << ',' << dictionary_access;
//  return stream.str();
  return (boost::format("%d,%d,%d,%d,%d,%d,%d,%d") %
    other %
    iterator_create %
    iterator_seq_access %
    iterator_increasing_access %
    iterator_random_access %
    accessor_create %
    accessor_access %
    dictionary_access).str();
}

template<typename T>
void SegmentAccessCounter::Counter<T>::reset() {
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
T SegmentAccessCounter::Counter<T>::sum() const {
  return other + iterator_create + iterator_seq_access + iterator_increasing_access + iterator_random_access +
         accessor_create + accessor_access + dictionary_access;
}

// Explicit instantiation of Counters used
template class SegmentAccessCounter::Counter<uint64_t>;
template class SegmentAccessCounter::Counter<std::atomic_uint64_t>;

SegmentAccessCounter::AccessPattern SegmentAccessCounter::_iterator_access_pattern(const std::shared_ptr<const PosList>& positions) {
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
void SegmentAccessCounter::reset(const std::map<std::string, std::shared_ptr<Table>>& tables) {
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        segment_ptr->access_counter.reset();
      }
    }
  }
}

void SegmentAccessCounter::on_iterator_create(uint64_t count) {
  ++_counter.iterator_create;
  _counter.iterator_seq_access += count;
}

/**
 * Increases the corresponding access counter [iterator_seq_access | iterator_increasing_access |
 * iterator_random_access] by positions->size(). The access counter is derived from the first N elements in positions
 * (see SegmentAccessStatisticsTools::iterator_access_pattern).
 */
void SegmentAccessCounter::on_iterator_create(const std::shared_ptr<const PosList>& positions) {
  ++_counter.iterator_create;
  const auto access_pattern = SegmentAccessCounter::_iterator_access_pattern(positions);
  switch (access_pattern) {
    case SegmentAccessCounter::AccessPattern::Unknown:
    case SegmentAccessCounter::AccessPattern::SeqInc:
    case SegmentAccessCounter::AccessPattern::SeqDec:
      _counter.iterator_seq_access += positions->size();
      break;
    case SegmentAccessCounter::AccessPattern::RndInc:
    case SegmentAccessCounter::AccessPattern::RndDec:
      _counter.iterator_increasing_access += positions->size();
      break;
    case SegmentAccessCounter::AccessPattern::Rnd:
      _counter.iterator_random_access += positions->size();
  }
}

// count is currently not in use.
void SegmentAccessCounter::on_accessor_create(uint64_t count) {
  ++_counter.accessor_create;
}

// chunk_offset is currently not in use. It may be used in the future to derive the access pattern.
void SegmentAccessCounter::on_accessor_access(uint64_t count, ChunkOffset chunk_offset) {
  _counter.accessor_access += count;
}

void SegmentAccessCounter::on_dictionary_access(uint64_t count) {
  _counter.dictionary_access += count;
}

void SegmentAccessCounter::on_other_access(uint64_t count) {
  _counter.other += count;
}

void SegmentAccessCounter::reset() {
  _counter.reset();
}

// returns a copy of the segments counter.
SegmentAccessCounter::Counter<uint64_t> SegmentAccessCounter::counter() const {
  SegmentAccessCounter::Counter<uint64_t> counter;
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