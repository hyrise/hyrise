#include <algorithm>
#include <chrono>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "hyrise.hpp"
#include "types.hpp"
#include "storage/table.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "benchmark_config.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/aggregate_hash.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "scheduler/job_task.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/value_segment.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

/**
 * CONFIGURATION
 * 
 * Edit this struct to change the run configuration without modifying the whole main().
 */
struct PlaygroundConfig {
  float scale_factor = 0.1f;    // TPC-H Scale Factor
  uint32_t num_workers = 4;     // Number of workers for Multi-Threaded variants
  uint32_t num_iterations = 5;  // Number of benchmark iterations per algorithm
  bool run_single_baseline = true;
  bool run_single_optimized = false;
  bool run_multi_naive = true;
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
 * Note: set_scheduler() calls finish() on old scheduler and begin() on new scheduler automatically.
 */
void setup_scheduler(bool multi_threaded, uint32_t worker_count = 1) {
  if (multi_threaded) {
    Hyrise::get().topology.use_non_numa_topology(worker_count);
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  } else {
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
  }
}

/**
 * HASH AGGREGATION IMPLEMENTATIONS
 */

std::shared_ptr<Table> hash_single_baseline(const std::shared_ptr<Table>& input) {
  // 1. Prepare Input
  auto table_wrapper = std::make_shared<TableWrapper>(input);
  table_wrapper->execute();

  // 2. Define Columns
  // Group By: l_returnflag, l_linestatus
  // Aggregate: SUM(l_quantity)
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  auto quantity_expr = std::make_shared<PQPColumnExpression>(quantity_col_id, input->column_data_type(quantity_col_id), input->column_is_nullable(quantity_col_id), input->column_name(quantity_col_id));

  // 3. Define Aggregate Expressions
  // usage: sum_(expression)
  auto sum_expr = expression_functional::sum_(quantity_expr);

  // 4. Create and Run Operator
  auto aggregate_op = std::make_shared<AggregateHash>(
      table_wrapper,
      std::vector<std::shared_ptr<WindowFunctionExpression>>{sum_expr},
      std::vector<ColumnID>{returnflag_col_id, linestatus_col_id}
  );

  aggregate_op->execute();

  return std::const_pointer_cast<Table>(aggregate_op->get_output());
}

std::shared_ptr<Table> hash_single_optimized(const std::shared_ptr<Table>& input) {
  // TODO: Implement single-threaded Hash Aggregation (Optimized)
  return nullptr; // Placeholder
}

std::shared_ptr<Table> hash_multi_naive(const std::shared_ptr<Table>& input) {
  const auto num_workers = CONFIG.num_workers;
  const auto chunk_count = input->chunk_count();

  // Column IDs
  const auto returnflag_col_id = input->column_id_by_name("l_returnflag");
  const auto linestatus_col_id = input->column_id_by_name("l_linestatus");
  const auto quantity_col_id = input->column_id_by_name("l_quantity");

  // Custom hash for pair<pmr_string, pmr_string>
  struct PairHash {
    std::size_t operator()(const std::pair<pmr_string, pmr_string>& p) const {
      auto h1 = std::hash<std::string_view>{}(p.first);
      auto h2 = std::hash<std::string_view>{}(p.second);
      return h1 ^ (h2 << 1);
    }
  };
  using AggregateKey = std::pair<pmr_string, pmr_string>;
  using PartialMap = std::unordered_map<AggregateKey, double, PairHash>;

  // Pre-allocate partial maps (one per worker, no synchronization needed)
  auto partial_maps = std::vector<PartialMap>(num_workers);

  // PHASE 1: Parallel aggregation with JobTasks
  auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
  jobs.reserve(num_workers);

  for (uint32_t worker_id = 0; worker_id < num_workers; ++worker_id) {
    jobs.emplace_back(std::make_shared<JobTask>([&, worker_id] {
      auto& local_map = partial_maps[worker_id];

      // Round-robin chunk assignment
      for (auto chunk_id = ChunkID{worker_id};
           static_cast<ChunkID::base_type>(chunk_id) < static_cast<ChunkID::base_type>(chunk_count);
           chunk_id = ChunkID{static_cast<ChunkID::base_type>(chunk_id) + num_workers}) {

        const auto chunk = input->get_chunk(chunk_id);
        if (!chunk) continue;

        const auto chunk_size = chunk->size();
        const auto& rf_seg = *chunk->get_segment(returnflag_col_id);
        const auto& ls_seg = *chunk->get_segment(linestatus_col_id);
        const auto& qty_seg = *chunk->get_segment(quantity_col_id);

        // Materialize values for this chunk
        auto rf_vals = std::vector<pmr_string>{};
        auto ls_vals = std::vector<pmr_string>{};
        auto qty_vals = std::vector<float>{};
        rf_vals.reserve(chunk_size);
        ls_vals.reserve(chunk_size);
        qty_vals.reserve(chunk_size);

        segment_iterate<pmr_string>(rf_seg, [&](const auto& pos) {
          rf_vals.push_back(pos.is_null() ? pmr_string{} : pmr_string{pos.value()});
        });
        segment_iterate<pmr_string>(ls_seg, [&](const auto& pos) {
          ls_vals.push_back(pos.is_null() ? pmr_string{} : pmr_string{pos.value()});
        });
        segment_iterate<float>(qty_seg, [&](const auto& pos) {
          qty_vals.push_back(pos.is_null() ? 0.0f : pos.value());
        });

        // Aggregate into local map
        for (size_t i = 0; i < rf_vals.size(); ++i) {
          auto key = std::make_pair(rf_vals[i], ls_vals[i]);
          local_map[key] += static_cast<double>(qty_vals[i]);
        }
      }
    }));
  }

  Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);

  // PHASE 2: Merge partial results (single-threaded)
  PartialMap merged_map;
  for (const auto& partial_map : partial_maps) {
    for (const auto& [key, sum] : partial_map) {
      merged_map[key] += sum;
    }
  }

  // PHASE 3: Build output table
  auto column_definitions = TableColumnDefinitions{
    TableColumnDefinition{"l_returnflag", DataType::String, false},
    TableColumnDefinition{"l_linestatus", DataType::String, false},
    TableColumnDefinition{"sum_quantity", DataType::Double, false}
  };

  auto output = std::make_shared<Table>(column_definitions, TableType::Data);

  auto rf_out = pmr_vector<pmr_string>{};
  auto ls_out = pmr_vector<pmr_string>{};
  auto sum_out = pmr_vector<double>{};

  rf_out.reserve(merged_map.size());
  ls_out.reserve(merged_map.size());
  sum_out.reserve(merged_map.size());

  for (const auto& [key, sum] : merged_map) {
    rf_out.push_back(key.first);
    ls_out.push_back(key.second);
    sum_out.push_back(sum);
  }

  auto segments = Segments{};
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(rf_out)));
  segments.push_back(std::make_shared<ValueSegment<pmr_string>>(std::move(ls_out)));
  segments.push_back(std::make_shared<ValueSegment<double>>(std::move(sum_out)));

  output->append_chunk(segments);

  return output;
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
 * HELPER: Compare two aggregation result tables
 * Since aggregation results can have rows in different orders, we convert to sets for comparison.
 */
bool verify_tables_equal(const Table& actual, const Table& expected) {
  // Check row counts
  if (actual.row_count() != expected.row_count()) {
    std::cerr << "    [VALIDATION FAILED] Row count mismatch: "
              << actual.row_count() << " vs " << expected.row_count() << std::endl;
    return false;
  }

  // Check column counts
  if (actual.column_count() != expected.column_count()) {
    std::cerr << "    [VALIDATION FAILED] Column count mismatch: "
              << actual.column_count() << " vs " << expected.column_count() << std::endl;
    return false;
  }

  // Convert both tables to set of rows (represented as strings for simplicity)
  auto table_to_set = [](const Table& table) {
    std::unordered_set<std::string> row_set;
    const auto chunk_count = table.chunk_count();

    // Iterate through all chunks
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = table.get_chunk(chunk_id);
      if (!chunk) continue;

      const auto chunk_size = chunk->size();
      const auto col_count = table.column_count();

      // Build vector of column values for each column
      std::vector<std::vector<std::string>> column_values(col_count);

      for (auto col_idx = ColumnID{0}; col_idx < col_count; ++col_idx) {
        const auto& segment = *chunk->get_segment(col_idx);
        const auto data_type = table.column_data_type(col_idx);

        column_values[col_idx].reserve(chunk_size);

        if (data_type == DataType::String) {
          segment_iterate<pmr_string>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::string(pos.value()));
          });
        } else if (data_type == DataType::Double) {
          segment_iterate<double>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::to_string(pos.value()));
          });
        } else if (data_type == DataType::Float) {
          segment_iterate<float>(segment, [&](const auto& pos) {
            column_values[col_idx].push_back(pos.is_null() ? "NULL" : std::to_string(pos.value()));
          });
        }
      }

      // Combine values for each row
      for (size_t row_in_chunk = 0; row_in_chunk < chunk_size; ++row_in_chunk) {
        std::string row_str;
        for (auto col_idx = ColumnID{0}; col_idx < col_count; ++col_idx) {
          row_str += column_values[col_idx][row_in_chunk] + "|";
        }
        row_set.insert(row_str);
      }
    }
    return row_set;
  };

  auto actual_rows = table_to_set(actual);
  auto expected_rows = table_to_set(expected);

  if (actual_rows != expected_rows) {
    std::cerr << "    [VALIDATION FAILED] Row content mismatch" << std::endl;
    return false;
  }

  return true;
}

/**
 * BENCHMARK DRIVER
 */
std::shared_ptr<Table> run_algorithm(const std::string& name,
                                     std::function<std::shared_ptr<Table>(const std::shared_ptr<Table>&)> func,
                                     const std::shared_ptr<Table>& input,
                                     const std::shared_ptr<Table>& expected_result) {
  const auto num_iterations = CONFIG.num_iterations;

  std::cout << "  Running " << name << " (" << num_iterations << " iterations)... " << std::flush;

  auto durations = std::vector<int64_t>{};
  durations.reserve(num_iterations);
  std::shared_ptr<Table> result = nullptr;

  for (uint32_t i = 0; i < num_iterations; ++i) {
    const auto start = std::chrono::high_resolution_clock::now();
    result = func(input);
    Hyrise::get().scheduler()->wait_for_all_tasks();  // Ensure async tasks are done
    const auto end = std::chrono::high_resolution_clock::now();

    const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    durations.push_back(duration);

    // Verify result on first iteration only
    if (i == 0 && expected_result && result) {
      if (!verify_tables_equal(*result, *expected_result)) {
        std::cout << "FAILED VALIDATION!" << std::endl;
        return result;
      } else {
        std::cout << "[VALIDATED] " << std::flush;
      }
    }
  }

  // Calculate statistics
  const auto sum = std::accumulate(durations.begin(), durations.end(), int64_t{0});
  const auto avg = static_cast<double>(sum) / static_cast<double>(num_iterations);
  const auto min = *std::min_element(durations.begin(), durations.end());
  const auto max = *std::max_element(durations.begin(), durations.end());

  std::cout << "Done." << std::endl;
  std::cout << "    Avg: " << std::fixed << std::setprecision(1) << avg << " ms"
            << " | Min: " << min << " ms"
            << " | Max: " << max << " ms" << std::endl;

  return result;
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
    expected_result = run_algorithm("Hash Single Baseline", hash_single_baseline, input_table, nullptr);
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
    expected_result = run_algorithm("Sort Single Baseline", sort_single_baseline, input_table, nullptr);
  }

  // 3. Single Threaded Optimized
  if (CONFIG.run_single_optimized) {
    setup_scheduler(false);
    std::cout << "  [Mode: Single Threaded]" << std::endl;
    run_algorithm("Sort Single Optimized", sort_single_optimized, input_table, expected_result);
  }

  // 4. Multi Baseline
  if (CONFIG.run_multi_naive) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Naive", sort_multi_naive, input_table, expected_result);
  }

  // 5. Multi Optimized
  if (CONFIG.run_multi_optimized) {
    setup_scheduler(true, CONFIG.num_workers);
    std::cout << "  [Mode: Multi Threaded (" << CONFIG.num_workers << " workers)]" << std::endl;
    run_algorithm("Sort Multi Optimized", sort_multi_optimized, input_table, expected_result);
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

  // Cleanup scheduler before exit
  Hyrise::get().scheduler()->finish();

  return 0;
}
