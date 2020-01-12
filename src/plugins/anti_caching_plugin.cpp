#include "anti_caching_plugin.hpp"

#include <iostream>

#include "hyrise.hpp"
#include "knapsack_solver.hpp"
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

  const auto timestamp = std::chrono::steady_clock::now();
  _segments = _fetch_segments();
  auto current_statistics = _fetch_current_statistics();
  if (!current_statistics.empty()) {
    _access_statistics.emplace_back(timestamp, std::move(current_statistics));
  }

  // knapsack problem here
  // MemoryBudget
  // Wert und Speicherbedarf pro Segment bestimmen.
  // Alle, die nicht in _access_statististics drin sind, haben den Wert 0
  // eigentlich benötigen wir ein Liste mit Segmenten
  // SegmentPtr, table_name, chunk_id, column_id
  // die erzuegen wir hier
  // Benötigen alle Segmente
  // Wie speichere ich alle counter weg?
  // loop über alle segmente, kopiere counter.
  // Was passiert, wenn ein Segment hinzukommt oder entfernt wird

  // gearbeitet wird auf segments und _access_statistics// konkreter auf AccessStatistics.back();
  // values = zugriffe
  // weights = size
  // memory budget
  // oder holen wir uns hier einfach genau die größen, die wir brauchen? Sprich ID, Zugriffe
}

std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> AntiCachingPlugin::_fetch_segments() {
  std::vector<std::pair<SegmentID, std::shared_ptr<BaseSegment>>> segments;
  const auto& tables = Hyrise::get().storage_manager.tables();
  for (const auto&[table_name, table_ptr] : tables) {
    for (auto chunk_id = ChunkID{0}, chunk_count = table_ptr->chunk_count(); chunk_id < chunk_count; ++chunk_id) {
      const auto& chunk_ptr = table_ptr->get_chunk(chunk_id);
      for (auto column_id = ColumnID{0}, column_count = static_cast<ColumnID>(chunk_ptr->column_count());
           column_id < column_count; ++column_id) {
        segments.emplace_back(SegmentID{table_name, chunk_id, column_id}, chunk_ptr->get_segment(column_id));
      }
    }
  }
  return segments;
}

std::vector<AntiCachingPlugin::SegmentIDAccessCounterPair> AntiCachingPlugin::_fetch_current_statistics() {
  std::vector<AntiCachingPlugin::SegmentIDAccessCounterPair> segment_id_access_counter_pairs;
  segment_id_access_counter_pairs.reserve(_segments.size());
  for (const auto& segment_id_segment_ptr_pair : _segments) {
    segment_id_access_counter_pairs.emplace_back(segment_id_segment_ptr_pair.first,
                                                 segment_id_segment_ptr_pair.second->access_statistics.counter());
  }
  return segment_id_access_counter_pairs;
}

void AntiCachingPlugin::export_access_statistics(const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {
//  auto entry_id = 0;
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

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum