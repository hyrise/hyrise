#include "segment_access_statistics.hpp"

#include <sstream>

#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/table.hpp"

namespace opossum {

SegmentAccessStatisticsTools::AccessPattern SegmentAccessStatisticsTools::iterator_access_pattern(const std::shared_ptr<const PosList>& positions) {
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

template <typename T>
std::string SegmentAccessCounter<T>::to_string() const {
  std::ostringstream stream;
  stream << other << ',' << iterator_create << ',' << iterator_seq_access << ',' << iterator_increasing_access << ','
    << iterator_random_access << ',' << accessor_create << ',' << accessor_access << ',' << dictionary_access;
  return stream.str();
}

template <typename T>
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

template <typename T>
T SegmentAccessCounter<T>::sum() const {
  return other + iterator_create + iterator_seq_access + iterator_increasing_access + iterator_random_access +
    accessor_create + accessor_access + dictionary_access;
}

SegmentAccessCounter<uint64_t> SegmentAccessStatisticsTools::fetch_counter(const std::shared_ptr<BaseSegment>& segment) {
  return segment->access_statistics.counter();
}

template class SegmentAccessCounter<uint64_t>;
template class SegmentAccessCounter<std::atomic_uint64_t>;

std::vector<SegmentAccessStatisticsTools::ColumnIDAccessStatisticsPair>
  SegmentAccessStatisticsTools::fetch_counters(const std::shared_ptr<Chunk>& chunk) {
  std::vector<SegmentAccessStatisticsTools::ColumnIDAccessStatisticsPair> column_id_access_statistics_pairs;

  for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk->column_count());
       column_id < count; ++column_id) {
    const auto& segment_ptr = chunk->get_segment(column_id);
    auto counter = fetch_counter(segment_ptr);
    if (counter.sum() > 0) {
      column_id_access_statistics_pairs.emplace_back(column_id, std::move(counter));
    }
  }

  return column_id_access_statistics_pairs;
}

std::vector<SegmentAccessStatisticsTools::ChunkIDColumnIDsPair> SegmentAccessStatisticsTools::fetch_counters(const std::shared_ptr<Table>& table) {
  std::vector<SegmentAccessStatisticsTools::ChunkIDColumnIDsPair> chunk_id_column_ids_pairs;

  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    auto column_id_access_statistics_pairs = fetch_counters(chunk);
    if (!column_id_access_statistics_pairs.empty()) {
      chunk_id_column_ids_pairs.emplace_back(chunk_id, std::move(column_id_access_statistics_pairs));
    }
  }

  return chunk_id_column_ids_pairs;
}


void SegmentAccessStatistics::on_iterator_create(uint64_t count) {
  ++_counter.iterator_create;
  _counter.iterator_seq_access += count;
//  std::cout << "on_iterator_create(uint64_t count)\n";
}

void SegmentAccessStatistics::on_iterator_create(const std::shared_ptr<const PosList>& positions) {
  ++_counter.iterator_create;
//  std::cout << "on_iterator_create(const std::shared_ptr<const PosList>& positions)\n";
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

void SegmentAccessStatistics::on_accessor_create(uint64_t count) {
  ++_counter.accessor_create;
}

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

void SegmentAccessStatistics::save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables,
                        const std::string& path_to_meta_data, const std::string& path_to_access_statistics) {
  auto entry_id = 0u;
  std::ofstream meta_file{path_to_meta_data};
  std::ofstream output_file{path_to_access_statistics};

    meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
    output_file << "entry_id," + SegmentAccessCounter<uint64_t>::HEADERS + "\n";
    // iterate over all tables, chunks and segments
    for (const auto&[table_name, table_ptr] : tables) {
      for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
        const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
             column_id < count; ++column_id) {
          const auto& column_name = table_ptr->column_name(column_id);
          const auto& segment_ptr = chunk_ptr->get_segment(column_id);
          const auto& access_counter = segment_ptr->access_statistics.counter();

          meta_file << entry_id << ',' << table_name << ',' << column_name << ',' << chunk_id << ','
                    << segment_ptr->size() << ',' << segment_ptr->estimate_memory_usage() << '\n';

          if (access_counter.sum() > 0) {
            output_file << entry_id << ',' << access_counter.to_string() << '\n';
          }
          ++entry_id;
        }
      }
    }

  meta_file.close();
  output_file.close();
}

/**
 * Resets access statistics of every segment in table
 * @param tables map of tables
 */
void SegmentAccessStatistics::reset_all(const std::map<std::string, std::shared_ptr<Table>>& tables) {
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



}  // namespace opossum