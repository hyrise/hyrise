#include <algorithm>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>

#include <boost/asio/ip/host_name.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_map.hpp>

#include "hyrise.hpp"
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  constexpr auto DEBUG = false;
  constexpr auto RUN_NLJ = false;

  constexpr auto CORE_COUNT = 8;
  constexpr auto RUN_COUNT = 10;

  constexpr auto BUILD_SIDE_SIZE = size_t{1'000'000};
  // constexpr auto BUILD_SIDE_SIZE = size_t{100'000'000};
  constexpr auto BUILD_PROBE_SIZE_FACTOR = size_t{10};
  constexpr auto PARTITION_COUNT = size_t{8};

  // static_assert(BUILD_SIDE_SIZE > 1'000);
  static_assert(BUILD_PROBE_SIZE_FACTOR > 1);
  static_assert(PARTITION_COUNT > 7);
  static_assert(PARTITION_COUNT >= CORE_COUNT);

  const auto hostname = boost::asio::ip::host_name();
  auto output_stream = std::ofstream{"join_measurements__" + hostname + ".csv"};
  output_stream << "HOSTNAME,JOIN,PHASE,PARTITION_COUNT,RUN_ID,RUNTIME_MS\n";

  auto random_device = std::random_device{};
  // auto generator = std::mt19937{17};
  auto generator = std::mt19937{};

  Hyrise::get().topology.use_non_numa_topology(CORE_COUNT);
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  for (auto partition_count = PARTITION_COUNT; partition_count <= 2048; partition_count *= 2) {
    std::cout << "Running with " << partition_count << " partitions.\n";

    // We have the offset list (elements * sizeof<pair<begin,end>>) + an assumed hash map blow up factor of 3 per entry
    // (again this pair). So we have 4*.
    const auto est_hash_map_size = (BUILD_SIDE_SIZE * sizeof(std::pair<size_t, size_t>) * 4) / partition_count;
    std::cout << "Estimated hash map size (per partition) is " << est_hash_map_size / 1000 << " KB.\n";

    auto build_input = std::vector<uint32_t>(BUILD_SIDE_SIZE);
    auto probe_input = std::vector<uint32_t>{};
    probe_input.reserve(BUILD_SIDE_SIZE * BUILD_PROBE_SIZE_FACTOR);

    std::iota(build_input.begin(), build_input.end(), 0);
    std::shuffle(build_input.begin(), build_input.end(), generator);
    // build_input.insert(build_input.end(), build_input.begin(), build_input.end());  // Non-unique.

    for (auto run = size_t{0}; run < 4; ++run) {
      probe_input.insert(probe_input.end(), build_input.begin(), build_input.end());
    }

    for (auto run_id = size_t{0}; run_id < RUN_COUNT; ++run_id) {
      std::cout << "Run " << run_id << ".\n";
      std::shuffle(build_input.begin(), build_input.end(), generator);
      std::shuffle(probe_input.begin(), probe_input.end(), generator);

      //
      //  Nested loop
      //
      auto nested_loop_result = std::vector<std::pair<size_t, size_t>>{};
      if (RUN_NLJ) {
        const auto nlj_start = std::chrono::steady_clock::now();
        auto build_row_id = size_t{0};
        for (const auto& build_item : build_input) {
          auto probe_row_id = size_t{0};

          for (const auto& probe_item : probe_input) {
            if (build_item == probe_item) {
              nested_loop_result.emplace_back(build_row_id, probe_row_id);
            }
            ++probe_row_id;
          }
          ++build_row_id;
        }
        const auto nlj_end = std::chrono::steady_clock::now();
        const auto nlj_duration = std::chrono::duration_cast<std::chrono::milliseconds>(nlj_end - nlj_start).count();
        output_stream << std::format("{},NestedLoopJoin,\"4 - Joining\",0,{},{}\n", hostname, run_id, nlj_duration);
      }
     

      // Simple single-threaded build of first stage hash table.
      const auto hjbg_start = std::chrono::steady_clock::now();
      auto hash_table = boost::unordered_multimap<uint32_t, size_t>{};
      hash_table.reserve(BUILD_SIDE_SIZE);
      auto position = size_t{0};
      for (const auto& build_side_item : build_input) {
        hash_table.emplace(build_side_item, position);
        ++position;
      }
      Assert(position == BUILD_SIDE_SIZE, "Unexpected count.");
      const auto hjbg_end = std::chrono::steady_clock::now();
      const auto hjbg_duration = std::chrono::duration_cast<std::chrono::milliseconds>(hjbg_end - hjbg_start).count();
      output_stream << std::format("{},HashJoin,\"1 - HashTableBuild\",{},{},{}\n", hostname, partition_count,run_id,hjbg_duration);

      // Finalize build tables.
      const auto hjbf_start = std::chrono::steady_clock::now();
      auto build_positions_vectors = std::vector<std::vector<size_t>>(partition_count);
      auto hash_tables = std::vector<boost::unordered_flat_map<uint32_t, std::pair<size_t, size_t>>>(partition_count);
      for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
        build_positions_vectors[partition_id].reserve((BUILD_SIDE_SIZE + 1) / partition_count);
        hash_tables[partition_id].reserve((BUILD_SIDE_SIZE + 1) / partition_count);
      }

      auto value_partition_id = size_t{0};
      auto key_positions = std::vector<size_t>{};
      auto previous_key = hash_table.begin()->first;
      for (const auto& [k, v] : hash_table) {
        if (k != previous_key) {
          // New key: flush.
          value_partition_id = previous_key % partition_count;
          hash_tables[value_partition_id].emplace(previous_key, std::make_pair(build_positions_vectors[value_partition_id].size(), build_positions_vectors[value_partition_id].size() + key_positions.size())); // for previous range
          build_positions_vectors[value_partition_id].insert(build_positions_vectors[value_partition_id].end(), key_positions.begin(), key_positions.end());
          key_positions.clear();
          previous_key = k;
        }
        key_positions.emplace_back(v);
      }
      value_partition_id = previous_key % partition_count;
      hash_tables[value_partition_id].emplace(previous_key, std::make_pair(build_positions_vectors[value_partition_id].size(), build_positions_vectors[value_partition_id].size() + key_positions.size()));
      build_positions_vectors[value_partition_id].insert(build_positions_vectors[value_partition_id].end(), key_positions.begin(), key_positions.end());
      const auto hjbf_end = std::chrono::steady_clock::now();
      const auto hjbf_duration = std::chrono::duration_cast<std::chrono::milliseconds>(hjbf_end - hjbf_start).count();
      output_stream << std::format("{},HashJoin,\"2 - HashTableFinalization\",{},{},{}\n", hostname, partition_count, run_id, hjbf_duration);

      const auto hjpp_start = std::chrono::steady_clock::now();
      // Probe side partitioning
      auto partitioned_probe_input = std::vector<std::vector<std::pair<size_t, size_t>>>(partition_count);
      for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
        partitioned_probe_input[partition_id].reserve((BUILD_SIDE_SIZE * BUILD_PROBE_SIZE_FACTOR + 1) / partition_count);
      }

      // Partition Probe Side.
      auto row_id = size_t{0};
      for (const auto& probe_item : probe_input) {
        const auto partition_id = probe_item % partition_count;
        partitioned_probe_input[partition_id].emplace_back(probe_item, row_id);
        ++row_id;
      }
      const auto hjpp_end = std::chrono::steady_clock::now();
      const auto hjpp_duration = std::chrono::duration_cast<std::chrono::milliseconds>(hjpp_end - hjpp_start).count();
      output_stream << std::format("{},HashJoin,\"3 - ProbeSidePartitioning\",{},{},{}\n", hostname, partition_count, run_id, hjpp_duration);

      if (DEBUG && RUN_NLJ) {
        std::cerr << "BUILD INPUT (positino:value):\n";
        for (auto index = size_t{0}; index < build_input.size(); ++index) {
          std::cerr << "(" << index << ":" << build_input[index] << ") ";
        }
        std::cerr << "\n\n";

        std::cerr << "PROBE INPUT (positino:value):\n";
        for (auto index = size_t{0}; index < probe_input.size(); ++index) {
          std::cerr << "(" << index << ":" << probe_input[index] << ") ";
        }
        std::cerr << "\n\n";

        std::cerr << "NL JOIN RESULT (row_id:row_id):\n";
        for (auto index = size_t{0}; index < nested_loop_result.size(); ++index) {
          std::cerr << "(" << nested_loop_result[index].first << ":" << nested_loop_result[index].second << ") ";
        }
        std::cerr << "\n\n";

        std::cerr << "HASH TABLE (value:row_id):\n";
        for (const auto& [k, v] : hash_table) {
          std::cerr << "(" << k << ":" << v << ") ";
        }
        std::cerr << "\n\n";

        for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
          std::cerr << "## Partition #" << partition_id << "\nBuild positions:\n";
          for (auto index = size_t{0}; index < build_positions_vectors[partition_id].size(); ++index) {
            std::cerr << build_positions_vectors[partition_id][index] << " ";
          }
          std::cerr << "\nHash Keys and Ranges:\n";
          auto& ht = hash_tables[partition_id];
          for (const auto& [k, v] : ht) {
            std::cerr << "(" << k << ":" << v.first << "&" << v.second << ") ";
          }
          std::cerr << "\nProbe Partition (value:row_id):\n";
          auto& pp = partitioned_probe_input[partition_id];
          for (const auto& [k, v] : pp) {
            std::cerr << "(" << k << ":" << v << ") ";
          }
          std::cerr << "\n";
        }
        std::cerr << "\n\n";
      }

      // Hash Join Join
      const auto hjj_start = std::chrono::steady_clock::now();
      auto result = std::vector<std::pair<size_t, size_t>>{};
      result.reserve(static_cast<size_t>(static_cast<double>(BUILD_SIDE_SIZE * BUILD_PROBE_SIZE_FACTOR) * 1.05));
      auto output_append_mutex = std::mutex{};
      auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
      for (auto partition_id = size_t{0}; partition_id < partition_count; ++partition_id) {
        tasks.emplace_back(std::make_shared<JobTask>([&, partition_id] {
          auto own_result = std::vector<std::pair<size_t, size_t>>{};
          own_result.reserve(static_cast<size_t>(static_cast<double>(BUILD_SIDE_SIZE * BUILD_PROBE_SIZE_FACTOR) * 1.05) / partition_count);
          const auto& own_probe_partition = partitioned_probe_input[partition_id];
          const auto& own_hash_table = hash_tables[partition_id];
          const auto& own_offsets = build_positions_vectors[partition_id];

          for (const auto& [value, row_id] : own_probe_partition) {
            auto lookup = own_hash_table.find(value);
            if (lookup != own_hash_table.end()) {
              const auto& [start, end] = lookup->second;
              for (auto index = start; index < end; ++index) {
                own_result.emplace_back(own_offsets[index], row_id);
              }
            }
          }

          const auto lock_guard = std::lock_guard<std::mutex>{output_append_mutex};
          result.insert(result.end(), own_result.begin(), own_result.end());
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
      const auto hjj_end = std::chrono::steady_clock::now();
      const auto hjj_duration = std::chrono::duration_cast<std::chrono::milliseconds>(hjj_end - hjj_start).count();
      output_stream << std::format("{},HashJoin,\"4 - Joining\",{},{},{}\n", hostname, partition_count, run_id, hjj_duration);

      if (RUN_NLJ) {
        std::sort(result.begin(), result.end());
        if (DEBUG) {
          std::cerr << "HASH JOIN RESULT (row_id:row_id):\n";
          for (auto index = size_t{0}; index < result.size(); ++index) {
            std::cerr << "(" << result[index].first << ":" << result[index].second << ") ";
          }
          std::cerr << "\n\n";
        }

        Assert(nested_loop_result.size() == result.size(), "Sizes do not match: " + std::to_string(nested_loop_result.size()) + " & " + std::to_string(result.size()));
        for (auto index = size_t{0}; index < nested_loop_result.size(); ++index) {
          Assert(nested_loop_result[index] == result[index], std::string{"Result "} + std::to_string(index) + " does not match.");  
        }    
      }
    }
  }

  output_stream.close();

  Hyrise::get().scheduler()->finish();

  return 0;
}
