#include "anti_caching_plugin.hpp"

#include <iostream>

#include "hyrise.hpp"
#include <storage/segment_access_statistics.hpp>

namespace opossum {

const std::string AntiCachingPlugin::description() const {
  return "AntiCaching Plugin";
}

void AntiCachingPlugin::start() {
  std::cout << "AntiCaching Plugin starting up.\n";
  _evaluate_statistics_thread =
    std::make_unique<PausableLoopThread>(REFRESH_STATISTICS_INTERVAL, [&](size_t) { _evaluate_statistics(); });

}

void AntiCachingPlugin::stop() {
  std::cout << "AntiCaching Plugin stopping.\n";

  _evaluate_statistics_thread.reset();
}

void AntiCachingPlugin::_evaluate_statistics() {
  std::cout << "Evaluating statistics\n";

  // get timestamp
  // for every segment get counters and reset
  // store counters
  // save to file
  // clear counters
  // do some random shit to analyze

  const auto timestamp = std::chrono::steady_clock::now();
  const auto& tables = Hyrise::get().storage_manager.tables();
  // iterate over all tables, chunks and segments
  // create timestamp vector<pair<ts, vector<pair<name>, pair<>>>>
    // <ts, table_map>
    // create table container
      // <table, chunk_map>
      // create chunk container
        // create column container

        // TimestampTableNamesPair
        // TableNameChunkIDsPair
        // vector<pair<chunk_id, vector<column_ids>>> -> ChunkIDColumnIDsPair
        // ChunkIDColumnIDsPair = pair<ChunkID, vector<ColumnIDAccessStatisticsPair>>
        // vector<ChunkIDColumnIDsPair>
        // ColumnIDAccessStatisticsPair
        // pair<ColumnID, std::vector<uint64_t>>
  for (const auto&[table_name, table_ptr] : tables) {
    auto table_name_chunk_ids_pair = TableNameChunkIDsPair{table_name, {}};
    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {

      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
      auto chunk_id_column_id_pair = ChunkIDColumnIDsPair{chunk_id, {}};
      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < count; ++column_id) {
        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
        auto& access_statistics = segment_ptr->access_statistics();
        auto counters = std::vector<uint64_t>{};
        counters.resize(static_cast<uint32_t>(SegmentAccessType::Count));
        auto sum = 0ul;
        for (auto type = 0u, end = static_cast<uint32_t>(SegmentAccessType::Count); type < end; ++type) {
          counters[type] = access_statistics.count(static_cast<SegmentAccessType>(type));
          sum += counters[type];
        }
        if (sum > 0) {
          chunk_id_column_id_pair.second.emplace_back(column_id, std::move(counters));
          access_statistics.reset();
        }
      }

      if (!chunk_id_column_id_pair.second.empty()) {
        table_name_chunk_ids_pair.second.push_back(std::move(chunk_id_column_id_pair));
      }
    }
    // hier geht es weiter

  }


}

void AntiCachingPlugin::export_access_statistics(const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {
  auto entry_id = 0;
  std::ofstream meta_file{path_to_meta_data};
  std::ofstream output_file{path_to_access_statistics};

//  meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
//  output_file << "entry_id," + AccessStrategyType::header() + "\n";
//  // iterate over all tables, chunks and segments
//  for (const auto&[table_name, table_ptr] : tables) {
//    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
//      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
//      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
//           column_id < count; ++column_id) {
//        const auto& column_name = table_ptr->column_name(column_id);
//        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
//        const auto& access_statistics = segment_ptr->access_statistics();
//
//        meta_file << entry_id << ',' << table_name << ',' << column_name << ',' << chunk_id << ','
//                  << segment_ptr->size() << ',' << segment_ptr->estimate_memory_usage() << '\n';
//
//        for (const auto& str : access_statistics._data_access_strategy.to_string()) {
//          output_file << entry_id << ',' << str << '\n';
//        }
//
//        ++entry_id;
//      }
//    }
//  }

  meta_file.close();
  output_file.close();
}

void AntiCachingPlugin::clear_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables) {

}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum