#include "clustering_plugin.hpp"

#include <iostream>
#include <filesystem>
#include <fstream>
#include <random>

#include "clustering/abstract_clustering_algo.hpp"
#include "clustering/simple_clustering_algo.hpp"
#include "clustering/chunkwise_clustering_algo.hpp"
#include "clustering/disjoint_clusters_algo.hpp"
#include "nlohmann/json.hpp"
#include "operators/update.hpp"
#include "operators/table_wrapper.hpp"
#include "resolve_type.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/base_attribute_statistics.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "utils/timer.hpp"

namespace opossum {

std::string ClusteringPlugin::description() const { return "This is the Hyrise ClusteringPlugin"; }

template <typename ColumnDataType>
std::pair<ColumnDataType,ColumnDataType> _get_min_max(const std::shared_ptr<BaseAttributeStatistics>& base_attribute_statistics) {
  const auto attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<ColumnDataType>>(base_attribute_statistics);
  Assert(attribute_statistics, "could not cast to AttributeStatistics");

  ColumnDataType min;
  ColumnDataType max;

  if constexpr (!std::is_arithmetic_v<ColumnDataType>) {
    Assert(attribute_statistics->min_max_filter, "no min-max filter despite non-arithmetic type");
    min = attribute_statistics->min_max_filter->min;
    max = attribute_statistics->min_max_filter->max;
  } else {
    Assert(attribute_statistics->range_filter, "no range filter despite arithmetic type");
    const auto ranges = attribute_statistics->range_filter->ranges;
    min = ranges[0].first;
    max = ranges.back().second;
  }

  return std::make_pair(min, max);
}

template <typename ColumnDataType>
std::pair<ColumnDataType,ColumnDataType> _get_min_max(const std::shared_ptr<Chunk>& chunk, const ColumnID column_id) {
  const auto pruning_statistics = chunk->pruning_statistics();
  Assert(pruning_statistics, "no pruning statistics");

  return _get_min_max<ColumnDataType>((*pruning_statistics)[column_id]);
}

void _export_chunk_pruning_statistics() {
  const std::string table_name{"lineitem"};
  if (!Hyrise::get().storage_manager.has_table(table_name)) return;
  std::cout << "[ClusteringPlugin] Exporting " <<  table_name << " chunk pruning stats...";

  const auto table = Hyrise::get().storage_manager.get_table(table_name);
  const std::vector<std::string> column_names = {"l_orderkey", "l_shipdate", "l_discount"};

  std::ofstream log(table_name + ".stats");

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (chunk) {
      for (const auto& column_name : column_names) {
        const auto column_id = table->column_id_by_name(column_name);
        const auto column_data_type = table->column_data_type(column_id);

        resolve_data_type(column_data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          const auto min_max = _get_min_max<ColumnDataType>(chunk, column_id);
          log << min_max.first << "," << min_max.second << "|";
        });
      }
      log << std::endl;
    }
  }
  std::cout << " Done" << std::endl;
}

void _export_chunk_size_statistics() {
  const std::string table_name = "lineitem";
  if (!Hyrise::get().storage_manager.has_table(table_name)) return;
  std::cout << "[ClusteringPlugin] Exporting " <<  table_name << " chunk size stats...";
  const auto& table = Hyrise::get().storage_manager.get_table(table_name);
  std::vector<size_t> chunk_sizes;

  for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); chunk_id++) {
    const auto& chunk = table->get_chunk(chunk_id);
    if (chunk) {
      chunk_sizes.push_back(chunk->size());
    }
  }

  std::ofstream log(table_name + ".cs");
  log << "[";
  for (const auto chunk_size : chunk_sizes) {
    log << chunk_size << ", ";
  }
  log << "]" << std::endl;

  std::cout << " Done" << std::endl;
}

void _encode_partition_chunks(const std::chrono::nanoseconds& interval) {
  auto& chunks_to_encode = Hyrise::get().chunks_to_encode;
  auto lineitem = Hyrise::get().storage_manager.get_table("lineitem");

  while(Hyrise::get().update_thread_state == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::cout << "Starting to encode partition chunks" << std::endl;

  while (Hyrise::get().update_thread_state < 6) {

    //std::cout << "Mutex locking"<< std::endl;
    Hyrise::get().chunks_to_encode_mutex->lock();
    //std::cout << "Mutex locked"<< std::endl;
    std::vector<ChunkID> chunk_ids(chunks_to_encode.cbegin(), chunks_to_encode.cend());
    chunks_to_encode.clear();
    Hyrise::get().chunks_to_encode_mutex->unlock();
    //std::cout << "Mutex unlocked"<< std::endl;

    for (const auto chunk_id : chunk_ids) {
      const auto& chunk = lineitem->get_chunk(chunk_id);
      if (chunk) {
          if (chunk->is_mutable()) chunk->finalize();
          ChunkEncoder::encode_chunk(chunk, lineitem->column_data_types(), EncodingType::Dictionary);
      } else {
        std::cout << "Weird behavior: chunks_to_encode contained a deleted chunk: " << chunk_id << std::endl;
      }
    }

    std::this_thread::sleep_for(interval);
  }
}

bool _can_delete_chunk(const std::shared_ptr<Chunk> chunk) {
  // Check whether there are still active transactions that might use the chunk
  Assert(chunk->get_cleanup_commit_id().has_value(), "expected a cleanup commit id");

  bool conflicting_transactions = false;
  auto lowest_snapshot_commit_id = Hyrise::get().transaction_manager.get_lowest_active_snapshot_commit_id();

  if (lowest_snapshot_commit_id.has_value()) {
    conflicting_transactions = chunk->get_cleanup_commit_id().value() > lowest_snapshot_commit_id.value();
  }

  return !conflicting_transactions;
}


void _physically_delete_chunks(const std::chrono::nanoseconds& interval) {
  auto& chunks_to_delete = Hyrise::get().chunks_to_delete;
  auto lineitem = Hyrise::get().storage_manager.get_table("lineitem");

  while(Hyrise::get().update_thread_state == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::cout << "Starting to physically delete invalidated chunks" << std::endl;

  while (Hyrise::get().update_thread_state < 6) {
    std::vector<ChunkID> deleted_chunks;
    //std::cout << "Mutex locking"<< std::endl;
    Hyrise::get().chunks_to_delete_mutex->lock();
    //std::cout << "Mutex locked"<< std::endl;
    for (const auto chunk_id : chunks_to_delete) {
      const auto& chunk = lineitem->get_chunk(chunk_id);
      if (chunk) {
        //std::cout << "checking deletability of chunk " << chunk_id << std::endl;
        if (_can_delete_chunk(chunk)) {
          //std::cout << "trying to delete chunk " << chunk_id << std::endl;
          lineitem->remove_chunk(chunk_id);
          deleted_chunks.push_back(chunk_id);
        }
      } else {
        std::cout << "Weird behavior: chunks_to_delete contained a deleted chunk: " << chunk_id << std::endl;
        chunks_to_delete.erase(chunk_id);
      }
    }

    for (const auto chunk_id : deleted_chunks) {
      chunks_to_delete.erase(chunk_id);
    }
    Hyrise::get().chunks_to_delete_mutex->unlock();
    //std::cout << "Mutex unlocked"<< std::endl;

    std::this_thread::sleep_for(interval);
  }
}


std::vector<size_t> _bytes_used;
std::vector<size_t> _time_offset;

void _track_memory_consumption(const std::chrono::nanoseconds& interval) {
  const std::string query {"SELECT SUM(estimated_size_in_bytes) FROm meta_segments WHERE table_name = 'lineitem'"};

  while(Hyrise::get().update_thread_state == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::cout << "Starting to track memory consumption" << std::endl;

  const auto start_time = std::chrono::system_clock::now();
  while (Hyrise::get().update_thread_state < 6) {
    auto pipeline = SQLPipelineBuilder{query}.create_pipeline();
    auto [status, table] = pipeline.get_result_table();
    Assert(status == SQLPipelineStatus::Success, "Could not calculate lineitem table size");
    std::optional<size_t> table_size = table->get_value<int64_t>(ColumnID{0}, 0);
    Assert(table_size, "table_size is null");
    const auto current_time = std::chrono::system_clock::now();
    _bytes_used.push_back(*table_size);
    _time_offset.push_back((current_time-start_time).count());
    //std::cout << "memory consumption captured, sleeping " << format_duration(interval) << std::endl;
    std::this_thread::sleep_for(interval);
  }
}



std::mutex _cout_mutex;

std::vector<size_t> _executed_updates(3, 0);
std::vector<size_t> _successful_executed_updates(3, 0);
std::mutex _update_mutex;
std::mutex _chunk_id_mutex;

template<typename S>
auto select_random(const S &s, size_t n) {
  auto it = std::begin(s);
  // 'advance' the iterator n times
  Assert(n < s.size(), "you advance too much");
  std::advance(it,n);

  return it;
}


#define MAX_UPDATES_PER_SECOND 10


void _update_rows_multithreaded(const size_t seed) {
  std::vector<size_t> executed_updates(3, 0);
  std::vector<size_t> successful_executed_updates(3, 0);
  const auto ideal_update_duration = std::chrono::nanoseconds(1'000'000'000 / MAX_UPDATES_PER_SECOND);


  while(Hyrise::get().update_thread_state == 0) {
    // wait for the clustering to begin
    std::this_thread::sleep_for (std::chrono::milliseconds(10));
  }

  _cout_mutex.lock();
  std::cout << "Thread " << seed << " started executing updates" << std::endl;
  _cout_mutex.unlock();

  std::unordered_set<ChunkID>& active_chunks = Hyrise::get().active_chunks;
  auto lineitem = Hyrise::get().storage_manager.get_table("lineitem");

  auto current_step = Hyrise::get().update_thread_state;
  std::mt19937 gen(seed);
  Timer timer;
  while (current_step < 4) {
    Hyrise::get().active_chunks_mutex->lock();
    std::uniform_int_distribution<> chunkidpos(0, active_chunks.size() - 1);
    const auto chunk_id = *select_random(active_chunks, chunkidpos(gen));
    Hyrise::get().active_chunks_mutex->unlock();
    const auto chunk = lineitem->get_chunk(chunk_id);
    Assert(chunk, "chunk " + std::to_string(chunk_id) + " does not exist");
    std::uniform_int_distribution<> chunkoffset(0, chunk->size() - 1);
    const ChunkOffset chunk_offset = chunkoffset(gen);
    if (chunk->mvcc_data()->get_end_cid(chunk_offset) != MvccData::MAX_COMMIT_ID) {
      //_cout_mutex.lock();
      //std::cout << "Thread " << seed <<  ": Row " << chunk_offset << " of chunk " << chunk_id << " is already invalid. Skipping it." << std::endl;
      //_cout_mutex.unlock();
      continue;
    }

    auto pos_list = std::make_shared<RowIDPosList>(1, RowID{chunk_id, chunk_offset});
    pos_list->guarantee_single_chunk();
    auto reference_table = std::make_shared<Table>(lineitem->column_definitions(), TableType::References);
    Segments segments;
    for (ColumnID column_id{0}; column_id < lineitem->column_count(); column_id++) {
      const auto segment = std::make_shared<ReferenceSegment>(lineitem, column_id, pos_list);
      segments.push_back(segment);
    }
    reference_table->append_chunk(segments);
    auto wrapper = std::make_shared<TableWrapper>(reference_table);
    wrapper->execute();

    auto update = std::make_shared<Update>("lineitem", wrapper, wrapper);
    auto transaction_context = Hyrise::get().transaction_manager.new_transaction_context(AutoCommit::No);
    update->set_transaction_context(transaction_context);
    update->execute();
    executed_updates[current_step - 1]++;
    if (!update->execute_failed()) {
      successful_executed_updates[current_step - 1]++;
      transaction_context->commit();
    } else {
      transaction_context->rollback(RollbackReason::Conflict);
      _cout_mutex.lock();
      std::cout << "Thread " << seed << ": Update failed at step " << current_step << std::endl;
      _cout_mutex.unlock();
    }

    auto new_step = Hyrise::get().update_thread_state;
    if (new_step != current_step) {
      _cout_mutex.lock();
      std::cout << "Thread " << seed << " step changes from " << current_step << " to " << new_step << std::endl;
      std::cout << "Thread " << seed << " executed " << executed_updates[current_step - 1]  << " updates in step " << current_step << std::endl;
      _cout_mutex.unlock();
    }

    current_step = new_step;

    const auto ns_used = timer.lap();
    if (current_step == 3 && ns_used < ideal_update_duration) {
      if (rand() % 100 == 0) {
        //std::cout << "Time: " << format_duration(ns_used) <<std::endl;
      }
      std::this_thread::sleep_for(std::chrono::nanoseconds(ideal_update_duration.count() - ns_used.count()));
    }
    timer.lap();
  }


  _cout_mutex.lock();
  std::cout << "Thread " << seed << " stopped executing updates" << std::endl;
  std::vector<std::string> step_names = {"partition", "merge", "sort"};
  for (size_t step_index = 0; step_index < step_names.size(); step_index++) {
    const auto total = executed_updates[step_index];
    const auto successful = successful_executed_updates[step_index];
    std::cout << "Thread with seed " << seed << " executed " << total << " updates during the " << step_names[step_index] << " step, " << successful << " (" << 100.0 * successful / total << "%) of them successful." << std::endl;


  }
  std::cout << executed_updates[0] << " " << executed_updates[1] << " " << executed_updates[2] << std::endl;
  std::cout << std::endl;
  _cout_mutex.unlock();

  _update_mutex.lock();
  for (size_t step{0}; step < 3; step++) {
    _executed_updates[step] += executed_updates[step];
    _successful_executed_updates[step] += successful_executed_updates[step];
  }
  _update_mutex.unlock();
}



void ClusteringPlugin::start() {
  _clustering_config = read_clustering_config();
  Hyrise::get().active_chunks_mutex = std::make_shared<std::mutex>();
  Hyrise::get().chunks_to_delete_mutex = std::make_shared<std::mutex>();
  Hyrise::get().chunks_to_encode_mutex = std::make_shared<std::mutex>();

  const auto env_var_algorithm = std::getenv("CLUSTERING_ALGORITHM");
  std::string algorithm;
  if (env_var_algorithm == NULL) {
    algorithm = "DisjointClusters";
  } else {
    algorithm = std::string(env_var_algorithm);
  }

  if (algorithm == "Partitioner") {
    _clustering_algo = std::make_shared<SimpleClusteringAlgo>(_clustering_config);
  } else if (algorithm == "DisjointClusters") {
    _clustering_algo = std::make_shared<DisjointClustersAlgo>(_clustering_config);
  } else {
    Fail("Unknown clustering algorithm: " + algorithm);
  }



  std::cout << "[ClusteringPlugin] Starting clustering, using " << _clustering_algo->description() << std::endl;

  constexpr size_t NUM_UPDATE_THREADS = 0;
  std::vector<std::thread> threads;
  std::cout << "Starting " << NUM_UPDATE_THREADS << " update threads" << std::endl;
  for (size_t thread_index = 0; thread_index < NUM_UPDATE_THREADS; thread_index++) {
    threads.push_back(std::thread(_update_rows_multithreaded, thread_index));
    std::cout << "Started thread " << thread_index << std::endl;
  }

  constexpr std::optional<std::chrono::nanoseconds> TRACK_MEMORY_CONSUMPTION_INTERVAL = std::chrono::seconds(8);
  std::thread memory_thread;
  if (TRACK_MEMORY_CONSUMPTION_INTERVAL) {
    std::cout << "Staring thread to track memory usage" << std::endl;
    memory_thread = std::thread(_track_memory_consumption, *TRACK_MEMORY_CONSUMPTION_INTERVAL);
  }


  constexpr std::optional<std::chrono::nanoseconds> PHYSICAL_DELETE_INTERVAL = std::chrono::milliseconds(1000);
  std::thread physical_delete_thread;
  if (PHYSICAL_DELETE_INTERVAL) {
    physical_delete_thread = std::thread(_physically_delete_chunks, *PHYSICAL_DELETE_INTERVAL);
  }

  constexpr std::optional<std::chrono::nanoseconds> PARTITION_ENCODING_INTERVAL = std::chrono::milliseconds(1000);
  std::thread partition_encoding_thread;
  if (PARTITION_ENCODING_INTERVAL) {
    partition_encoding_thread = std::thread(_encode_partition_chunks, *PARTITION_ENCODING_INTERVAL);
  }

  _clustering_algo->run();

  std::vector<std::string> step_names = {"partition", "merge", "sort"};
  for (size_t step = 0; step < step_names.size(); step++) {
    const auto total = _executed_updates[step];
    const auto successful = _successful_executed_updates[step];
    //const std::string runtime = _clustering_algo->runtime_statistics()["lineitem"]["steps"][step_names[step]];
    //const auto seconds = runtime / 1e9;
    std::cout << "Executed " << total << " updates in " << _clustering_algo->runtime_statistics()["lineitem"]["steps"][step_names[step]] << "s during the " << step_names[step] << " step, " << successful << " (" << 100.0 * successful / total << "%) of them successful." << std::endl;
  }

  for (size_t thread_index = 0; thread_index < NUM_UPDATE_THREADS; thread_index++) {
    threads[thread_index].join();
    std::cout << "Stopped thread " << thread_index << std::endl;
  }

  if (PHYSICAL_DELETE_INTERVAL) {
    std::cout << "Stopped physical delete thread" << std::endl;
    physical_delete_thread.join();
  }

  if (PARTITION_ENCODING_INTERVAL) {
    std::cout << "Stopped partition encoding thread" << std::endl;
    partition_encoding_thread.join();
  }

  if (TRACK_MEMORY_CONSUMPTION_INTERVAL) {
    std::cout << "Stopped memory usage tracking thread" << std::endl;
    memory_thread.join();

    std::cout << "bytes_used";
    if (PHYSICAL_DELETE_INTERVAL) std::cout << "_delete";
    if (PARTITION_ENCODING_INTERVAL) std::cout << "_encoded";
    std::cout << " = [";
    for (const auto bytes : _bytes_used) {
      std::cout << bytes << ", ";
    }
    std::cout << "]" << std::endl;

    std::cout << "times";
    if (PHYSICAL_DELETE_INTERVAL) std::cout << "_delete";
    if (PARTITION_ENCODING_INTERVAL) std::cout << "_encoded";
    std::cout << " = [";
    for (const auto offset : _time_offset) {
      std::cout << offset << ", ";
    }
    std::cout << "]" << std::endl;

    nlohmann::json step_times = _clustering_algo->runtime_statistics()["lineitem"]["steps"];
    size_t offset_partition_end = step_times["partition"];

    size_t offset_merge_end = step_times["merge"];
    offset_merge_end += offset_partition_end;

    size_t offset_sort_end = step_times["sort"];
    offset_sort_end += offset_merge_end;

    size_t offset_encode_end = step_times["encode"];
    offset_encode_end += offset_sort_end;


    std::cout << "phase_ends";
    if (PHYSICAL_DELETE_INTERVAL) std::cout << "_delete";
    if (PARTITION_ENCODING_INTERVAL) std::cout << "_encoded";
    std::cout << " = [" << offset_partition_end << ", " << offset_merge_end << ", " << offset_sort_end << ", " << offset_encode_end << "]" << std::endl;
  }

  _write_clustering_information();

  //_export_chunk_pruning_statistics();
  //_export_chunk_size_statistics();
  std::cout << "[ClusteringPlugin] Clustering complete." << std::endl;
}

void ClusteringPlugin::stop() { }

const ClusteringByTable ClusteringPlugin::read_clustering_config(const std::string& filename) {
  if (!std::filesystem::exists(filename)) {
    std::cout << "clustering config file not found: " << filename << std::endl;
    std::exit(1);
  }

  std::ifstream ifs(filename);
  const auto clustering_config = nlohmann::json::parse(ifs);
  return clustering_config;
}

void ClusteringPlugin::_write_clustering_information() const {
  nlohmann::json clustering_info;
  clustering_info["runtime"] = _clustering_algo->runtime_statistics();
  clustering_info["config"] = _clustering_config;
  clustering_info["algo"] = _clustering_algo->description();

  std::ofstream out_file(".clustering_info.json");
  out_file << clustering_info.dump(2) << std::endl;
  out_file.close();
}

EXPORT_PLUGIN(ClusteringPlugin)

}  // namespace opossum
