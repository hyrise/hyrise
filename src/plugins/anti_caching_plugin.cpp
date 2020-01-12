#include "anti_caching_plugin.hpp"

#include <iostream>
#include <numeric>

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
  // oder holen wir uns hier einfach genau die größen, die wir brauchen? Sprich ID, Zugriffe (ja)
  _evict_segments();


}

void AntiCachingPlugin::_evict_segments() {
  if (_access_statistics.empty()) {
    std::cout << "No segments found.";
    return;
  }

  const auto& access_statistics = _access_statistics.back().second;
  // values ermitteln
  std::vector<float> values(access_statistics.size());
  std::vector<size_t> memory_usages(access_statistics.size());
  for (size_t index = 0, end = access_statistics.size(); index < end; ++index) {
    const auto& segment_info = access_statistics[index];
    values[index] = _compute_value(segment_info);
    memory_usages[index] = segment_info.memory_usage;
  }

  const auto selected_indices = KnapsackSolver::solve(memory_budget, values, memory_usages);

  // knapsack solver ausführen
  // printen, was evicted wurde.
  const auto total_size = std::accumulate(memory_usages.cbegin(), memory_usages.cend(), 0);
  const auto selected_size = std::accumulate(selected_indices.cbegin(), selected_indices.cend(), 0,
                                             [&memory_usages](const size_t a, const size_t b) {
                                               return memory_usages[a] + memory_usages[b];
                                             });

  std::cout << access_statistics.size() << " segments (" << total_size / 1048576 << " MB), " << (access_statistics.size() - selected_indices.size())
            << " evicted (" << (total_size - selected_size) / 1048576 << " MB )\n";
}

float AntiCachingPlugin::_compute_value(const SegmentInfo& segment_info) {
  // data type spielt vermutlich auch eine Rolle
  const auto& counter = segment_info.access_counter;
  const auto seq_access_factor = 1.0f;
  const auto seq_increasing_access_factor = 1.2f;
  const auto rnd_access_factor = 900'000.0f / 350'000.0f;
  const auto accessor_access_factor = rnd_access_factor;
  const auto dictionary_access_factor = 0.0f;
  // Zugriffe * Zugriffsart // faktor für zugriffsart
  return counter.accessor_access * accessor_access_factor + counter.iterator_seq_access * seq_access_factor +
         counter.iterator_increasing_access + seq_increasing_access_factor +
         counter.iterator_random_access * rnd_access_factor +
         counter.other * rnd_access_factor + counter.dictionary_access * dictionary_access_factor;
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

std::vector<SegmentInfo> AntiCachingPlugin::_fetch_current_statistics() {
  std::vector<SegmentInfo> access_statistics;
  const auto segments = AntiCachingPlugin::_fetch_segments();
  access_statistics.reserve(segments.size());
  for (const auto& segment_id_segment_ptr_pair : segments) {
    access_statistics.emplace_back(segment_id_segment_ptr_pair.first.table_name,
                                   segment_id_segment_ptr_pair.first.chunk_id,
                                   segment_id_segment_ptr_pair.first.column_id,
                                   segment_id_segment_ptr_pair.second->estimate_memory_usage(),
                                   segment_id_segment_ptr_pair.second->access_statistics.counter());
  }
  return access_statistics;
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