
#include "segment_access_counter.hpp"
#include "types.hpp"

#include <fstream>
#include <iostream>

namespace opossum {

const SegmentAccessCounter::AccessStatistics SegmentAccessCounter::no_statistics{};

SegmentAccessCounter& SegmentAccessCounter::instance() {
  static SegmentAccessCounter instance;
  return instance;
}

void SegmentAccessCounter::increase(const uint32_t segment_id, AccessType type) {
  std::lock_guard<std::mutex> lock(_statistics_lock);
  ++_statistics[segment_id].count[type];
}

const SegmentAccessCounter::AccessStatistics& SegmentAccessCounter::statistics(const uint32_t segment_id) const {
  const auto id_statistics_pair = _statistics.find(segment_id);
  if (id_statistics_pair == _statistics.cend()) {
    return SegmentAccessCounter::no_statistics;
  }
  return id_statistics_pair->second;
}

void SegmentAccessCounter::save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables,
                                       const std::string& path) const {
  std::ofstream output_file{path};
  // iterate over all tables, chunks and segments
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& column_name = table_ptr->column_name(column_id);
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        const auto& access_statistics = statistics(segment_ptr->id());
        // table_name.column_name.segment_id,type1,type2,...
        output_file << table_name << '.' << column_name << '.' << segment_ptr->id() << ','
                    << access_statistics.to_string() << std::endl;
      }
    }
  }

  for (auto& [key, value] : _statistics)
    output_file << "key" << key << ": " << value.to_string() << std::endl;

  output_file.close();
}

}  // namespace opossum