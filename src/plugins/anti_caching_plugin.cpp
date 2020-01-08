#include "anti_caching_plugin.hpp"

#include <iostream>

#include "hyrise.hpp"

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

  const auto& tables = Hyrise::get().storage_manager.tables();

  // get timestamp
  // for every segment get counters and reset
  // store counters
  // save to file
  // clear counters
  // do some random shit to analyze


// iterate over all tables, chunks and segments
//  for (const auto&[table_name, table_ptr] : tables) {
//    for (auto chunk_id = ChunkID{0}; chunk_id < table_ptr->chunk_count(); ++chunk_id) {
//      const auto chunk_ptr = table_ptr->get_chunk(chunk_id);
//      for (auto column_id = ColumnID{0}, count = static_cast<ColumnID>(chunk_ptr->column_count());
//           column_id < count; ++column_id) {
//        const auto& column_name = table_ptr->column_name(column_id);
//        const auto& segment_ptr = chunk_ptr->get_segment(column_id);
//        const auto& access_statistics = segment_ptr->access_statistics();
//          // copy access statistics to local storage.
//      }
//    }
//  }


}

void AntiCachingPlugin::export_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables,
                                                 const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {

}

void AntiCachingPlugin::clear_access_statistics(const std::map<std::string, std::shared_ptr<Table>>& tables) {

}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum