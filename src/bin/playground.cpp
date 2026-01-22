#include <iostream>
#include <memory>
#include <vector>
#include <string>
#include <chrono>
#include <unordered_map>
#include <functional>

#include "hyrise.hpp"
#include "types.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "benchmark_config.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * CONFIGURATION
 * 
 * Edit this struct to change the run configuration without modifying the whole main().
 */
struct PlaygroundConfig {
  float scale_factor = 0.1f;    // TPC-H Scale Factor
  uint32_t num_workers = 1;     // Number of workers for Multi-Threaded variants
  bool run_single_baseline = true;
  bool run_single_optimized = false;
  bool run_multi_naive = false;
  bool run_multi_optimized = false;
};

// Global Config Instance
const auto CONFIG = PlaygroundConfig{};



std::shared_ptr<Table> generate_lineitem_data(float scale_factor) {
  std::cout << "- Generating TPC-H LineItem (SF " << scale_factor << ")..." << std::endl;
  auto generator = TPCHTableGenerator{scale_factor, ClusteringConfiguration::None};
  auto tables = generator.generate();
  return tables["lineitem"].table;
}

/**
 * SCHEDULER SETUP
 */
void setup_scheduler(bool multi_threaded, uint32_t worker_count = 1) {
  if (multi_threaded) {
    Hyrise::get().topology.use_fake_numa_topology(worker_count, 1);
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
    Hyrise::get().scheduler()->begin(); // IMPORTANT: Start the workers
  } else {
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
  }
}

/**
 * HASH AGGREGATION IMPLEMENTATIONS
 */

std::shared_ptr<Table> hash_single_baseline(const std::shared_ptr<Table>& input) {
  // TODO: Implement single-threaded Hash Aggregation (Baseline)
  return nullptr; // Placeholder
}

std::shared_ptr<Table> hash_single_optimized(const std::shared_ptr<Table>& input) {
  // TODO: Implement single-threaded Hash Aggregation (Optimized)
  return nullptr; // Placeholder
}

std::shared_ptr<Table> hash_multi_naive(const std::shared_ptr<Table>& input) {
  // TODO: Implement multi-threaded Hash Aggregation (Naive)
  // Use JobTask
  return nullptr; // Placeholder
}

std::shared_ptr<Table> hash_multi_optimized(const std::shared_ptr<Table>& input) {
  // TODO: Implement multi-threaded Hash Aggregation (Optimized)
  return nullptr; // Placeholder
}


/**
 * SORT AGGREGATION IMPLEMENTATIONS
 */

std::shared_ptr<Table> sort_single_baseline(const std::shared_ptr<Table>& input) {
  // TODO: Implement single-threaded Sort Aggregation (Baseline)
  return nullptr;
}

std::shared_ptr<Table> sort_single_optimized(const std::shared_ptr<Table>& input) {
  // TODO: Implement single-threaded Sort Aggregation (Optimized)
  return nullptr;
}

std::shared_ptr<Table> sort_multi_naive(const std::shared_ptr<Table>& input) {
  // TODO: Implement multi-threaded Sort Aggregation (Naive)
  // Use JobTask
  return nullptr;
}

std::shared_ptr<Table> sort_multi_optimized(const std::shared_ptr<Table>& input) {
  // TODO: Implement multi-threaded Sort Aggregation (Optimized)
  return nullptr;
}


/**
 * BENCHMARK DRIVER
 */
void run_algorithm(const std::string& name, 
                   std::function<std::shared_ptr<Table>(const std::shared_ptr<Table>&)> func, 
                   const std::shared_ptr<Table>& input,
                   const std::shared_ptr<Table>& expected_result) {
  
  std::cout << "  Running " << name << "... " << std::flush;
  
  const auto start = std::chrono::high_resolution_clock::now();
  auto result = func(input);
  Hyrise::get().scheduler()->wait_for_all_tasks(); // Ensure async tasks are done
  const auto end = std::chrono::high_resolution_clock::now();
  
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "Done (" << duration << " ms)." << std::endl;

  if (expected_result && result) {
      // TODO: Implement verify_tables_equal(*result, *expected_result);
      // For now we just check usage
      // Assert(result->row_count() == expected_result->row_count(), "Row counts mismatch");
  }
}

void run_hash_micro_benchmark(float scale_factor) {
  std::cout << "=== Running HASH Microbenchmark ===" << std::endl;
  
  // 1. Data Generation
  auto input_table = generate_lineitem_data(scale_factor);
  std::shared_ptr<Table> expected_result = nullptr;

  // 2. Single Threaded Baseline (Ground Truth)
  if (CONFIG.run_single_baseline) {
    setup_scheduler(false); // Single
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Hash Single Baseline", hash_single_baseline, input_table, nullptr);
    // expected_result = ...; // Store this result
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Hash Single Optimized", hash_single_optimized, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Hash Multi Naive", hash_multi_naive, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Hash Multi Optimized", hash_multi_optimized, input_table, expected_result);
  }
  
  std::cout << "=== HASH Microbenchmark Finished ===" << std::endl;
}

void run_sort_micro_benchmark(float scale_factor) {
  std::cout << "=== Running SORT Microbenchmark ===" << std::endl;
  // 1. Data Generation
  auto input_table = generate_lineitem_data(scale_factor);
  std::shared_ptr<Table> expected_result = nullptr;

  // 2. Single Threaded Baseline (Ground Truth)
  if (CONFIG.run_single_baseline) {
    setup_scheduler(false); // Single
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Sort Single Baseline", hash_single_baseline, input_table, nullptr);
    // expected_result = ...; // Store this result
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Sort Single Optimized", hash_single_optimized, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Naive", hash_multi_naive, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Optimized", hash_multi_optimized, input_table, expected_result);
  }

  std::cout << "=== SORT Microbenchmark Finished ===" << std::endl;
}


/**
 * MAIN
 */
int main() {
  std::cout << "Hyrise Playground: Hash vs Sort Aggregation" << std::endl;
  
  // Just uncomment/comment the parts you want to run
  run_hash_micro_benchmark(CONFIG.scale_factor);
  // run_sort_micro_benchmark(CONFIG.scale_factor);

  return 0;
}
