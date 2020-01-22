#include "anti_caching_plugin.hpp"

#include <ctime>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <numeric>

#include "boost/format.hpp"
#include "hyrise.hpp"
#include "knapsack_solver.hpp"
#include <storage/segment_access_counter.hpp>

namespace opossum {

namespace anticaching {

bool SegmentID::operator==(const SegmentID& other) const {
  return table_name == other.table_name && chunk_id == other.chunk_id && column_id == other.column_id &&
         column_name == other.column_name;
}

size_t SegmentIDHasher::operator()(const SegmentID& segment_id) const {
  size_t res = 17;
  res = res * 31 + std::hash<std::string>()(segment_id.table_name);
  res = res * 31 + std::hash<ChunkID>()(segment_id.chunk_id);
  res = res * 31 + std::hash<ColumnID>()(segment_id.column_id);
  res = res * 31 + std::hash<std::string>()(segment_id.column_name);
  return res;
}

}

AntiCachingPlugin::AntiCachingPlugin() {
  _log_file.open("anti_caching_plugin.log", std::ofstream::app);
  _log_line("Plugin created");
}

AntiCachingPlugin::~AntiCachingPlugin() {
  _log_line("Plugin destroyed");
}

const std::string AntiCachingPlugin::description() const {
  return "AntiCaching Plugin";
}

void AntiCachingPlugin::start() {

  _log_line("Starting Plugin");
  _evaluate_statistics_thread =
    std::make_unique<PausableLoopThread>(REFRESH_STATISTICS_INTERVAL, [&](size_t) { _evaluate_statistics(); });

}

void AntiCachingPlugin::stop() {
  _log_line("Stopping Plugin");
  _evaluate_statistics_thread.reset();
}

void AntiCachingPlugin::_evaluate_statistics() {
  _log_line("Evaluating statistics start");

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
  _log_line("Evaluating statistics end");
}

void AntiCachingPlugin::_evict_segments() {
  if (_access_statistics.empty()) {
    _log_line("No segments found.");
    return;
  }

  const auto& access_statistics = _access_statistics.back().second;
  // values ermitteln
  std::vector<float> values(access_statistics.size());
  std::vector<size_t> memory_usages(access_statistics.size());
  // initialized here, will be uses later to output evicted segments
  std::unordered_set<size_t> evicted_indices;
  for (size_t index = 0, end = access_statistics.size(); index < end; ++index) {
    const auto& segment_info = access_statistics[index];
    values[index] = _compute_value(segment_info);
    memory_usages[index] = segment_info.memory_usage;
    evicted_indices.emplace(index);
  }

  const auto selected_indices = KnapsackSolver::solve(memory_budget, values, memory_usages);

  // knapsack solver ausführen
  // printen, was evicted wurde.
  const auto total_size = std::accumulate(memory_usages.cbegin(), memory_usages.cend(), 0);
  const auto selected_size = std::accumulate(selected_indices.cbegin(), selected_indices.cend(), 0,
                                             [&memory_usages](const size_t sum, const size_t index) {
                                               return sum + memory_usages[index];
                                             });

  for (const auto index : selected_indices) {
    evicted_indices.erase(index);
  }
  DebugAssert(evicted_indices.size() == access_statistics.size() - selected_indices.size(),
              "|evicted_indices| should be equal to total_size - selected_size.");
  const auto bytes_in_mb = 1024.0f * 1024.0f;
  const auto evicted_memory = ((total_size - selected_size) / bytes_in_mb);
  _log_line((boost::format(
    "%d of %d segments evicted. %f MB of %f MB evicted. %f%% of memory budget used (memory budget: %f MB).") %
             (access_statistics.size() - selected_indices.size()) %
             access_statistics.size() %
             evicted_memory %
             (total_size / bytes_in_mb) %
             (100.0f * selected_size / memory_budget) %
             (memory_budget / bytes_in_mb)).str());
  for (const auto index : evicted_indices) {
    const auto& segment_info = access_statistics[index];
    _log_line((boost::format("%s.%s (chunk_id: %d, access_count; %d) evicted.") %
               segment_info.segment_id.table_name % segment_info.segment_id.column_name %
               segment_info.segment_id.chunk_id % segment_info.access_counter.sum()).str());
  }
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
        const auto& column_name = table_ptr->column_name(column_id);
        segments.emplace_back(SegmentID{table_name, chunk_id, column_id, column_name},
                              chunk_ptr->get_segment(column_id));
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
    access_statistics.emplace_back(segment_id_segment_ptr_pair.first,
                                   segment_id_segment_ptr_pair.second->memory_usage(MemoryUsageCalculationMode::Sampled),
                                   segment_id_segment_ptr_pair.second->size(),
                                   segment_id_segment_ptr_pair.second->access_counter.counter());
  }
  return access_statistics;
}

void AntiCachingPlugin::export_access_statistics(const std::string& path_to_meta_data,
                                                 const std::string& path_to_access_statistics) {
  uint32_t entry_id_counter = 0u;

  std::ofstream meta_file{path_to_meta_data};
  std::ofstream output_file{path_to_access_statistics};
  std::unordered_map<SegmentID, uint32_t, anticaching::SegmentIDHasher> segment_id_entry_id_map;

  meta_file << "entry_id,table_name,column_name,chunk_id,row_count,EstimatedMemoryUsage\n";
  output_file << "entry_id," + SegmentAccessCounter::Counter<uint64_t>::HEADERS + "\n";

  for (const auto& timestamp_segment_info_pair : _access_statistics) {
    const auto& timestamp = timestamp_segment_info_pair.first;
    const auto elapsed_time = timestamp - _initialization_time;
    const auto& segment_infos = timestamp_segment_info_pair.second;
    for (const auto& segment_info : segment_infos) {
      const auto stored_entry_id_it = segment_id_entry_id_map.find(segment_info.segment_id);
      uint32_t entry_id = 0u;
      if (stored_entry_id_it != segment_id_entry_id_map.cend()) entry_id = stored_entry_id_it->second;
      else {
        entry_id = entry_id_counter++;
        // TODO: size and memory_usage are only written once and never updated. This should be changed in the future.
        meta_file << entry_id << ',' << segment_info.segment_id.table_name << ',' << segment_info.segment_id.column_name
                  << ',' << segment_info.segment_id.chunk_id << ',' << segment_info.size << ','
                  << segment_info.memory_usage << '\n';
      }
      output_file << entry_id << ','
                  << std::chrono::duration_cast<std::chrono::seconds>(elapsed_time).count()
                  << segment_info.access_counter.to_string() << '\n';
    }
  }

  meta_file.close();
  output_file.close();
}

void AntiCachingPlugin::_log_line(const std::string& text) {
  const auto timestamp = std::time(nullptr);
  const auto local_time = std::localtime(&timestamp);
  _log_file << std::put_time(local_time, "%d.%m.%Y %H:%M:%S") << ", " << text << '\n';
}

EXPORT_PLUGIN(AntiCachingPlugin)

} // namespace opossum