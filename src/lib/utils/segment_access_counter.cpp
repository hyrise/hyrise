
#include "segment_access_counter.hpp"
#include "types.hpp"

#include <fstream>
#include <iostream>

namespace opossum {

void SegmentAccessCounter::save_to_csv(const std::map<std::string, std::shared_ptr<Table>>& tables,
                                       const std::string& path) {
  std::ofstream output_file{path};
  output_file << "table_name,column_name,chunk_id,row_count,Other,IteratorCreate,IteratorAccess,AccessorCreate,"
                 "AccessorAccess,DictionaryAccess\n";
  // iterate over all tables, chunks and segments
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& column_name = table_ptr->column_name(column_id);
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        const auto& access_statistics = segment_ptr->access_statistics();
        output_file << table_name << ',' << column_name << ',' << chunk_id << ',' << segment_ptr->size() << ','
                    << access_statistics.to_string() << '\n';
      }
    }
  }

//  for (auto&[key, value] : _statistics)
//    output_file << "key" << key << ": " << value.to_string() << std::endl;
  output_file.close();
}

void SegmentAccessCounter::reset(const std::map<std::string, std::shared_ptr<Table>>& tables) {
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        segment_ptr->access_statistics().reset_all();
      }
    }
  }
}

}  // namespace opossum