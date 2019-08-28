#pragma once

#include <json.hpp>

#include <atomic>
#include <chrono>
#include <iostream>
#include <optional>
#include <unordered_map>
#include <vector>

#include "cxxopts.hpp"
#include "tbb/concurrent_hash_map.h"

#include "abstract_benchmark_item_runner.hpp"
#include "abstract_table_generator.hpp"
#include "benchmark_item_result.hpp"
#include "benchmark_state.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "operators/abstract_operator.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "sql/sql_plan_cache.hpp"
#include "storage/chunk.hpp"
#include "storage/encoding_type.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

class SQLPipeline;
struct SQLPipelineMetrics;
class SQLiteWrapper;

// The BenchmarkRunner is the main class for the benchmark framework. It gets initialized by the benchmark binaries
// (e.g., tpch_benchmark.cpp). They then hand over the control to the BenchmarkRunner (inversion of control), which
// calls the supplied table generator, runs and times the benchmark items, and reports the benchmark results.
class BenchmarkRunner : Noncopyable {
 public:
  BenchmarkRunner(const BenchmarkConfig& config, std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner,
                  std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context);

  void run();

  static cxxopts::Options get_basic_cli_options(const std::string& benchmark_name);

  static nlohmann::json create_context(const BenchmarkConfig& config);

  // If the query execution should be validated, this stores a pointer to the used SQLite instance
  std::shared_ptr<SQLiteWrapper> sqlite_wrapper;

 private:
  // Run benchmark in BenchmarkMode::Shuffled mode
  void _benchmark_shuffled();

  // Run benchmark in BenchmarkMode::Ordered mode
  void _benchmark_ordered();

  // Execute warmup run of a benchmark item
  void _warmup(const BenchmarkItemID item_id);

  // Schedules a run of the specified for execution. After execution, the result is updated. If the scheduler is
  // disabled, the item is executed immediately.
  void _schedule_item_run(const BenchmarkItemID item_id);

  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json
  void _create_report(std::ostream& stream) const;

  const BenchmarkConfig _config;

  std::unique_ptr<AbstractBenchmarkItemRunner> _benchmark_item_runner;
  std::unique_ptr<AbstractTableGenerator> _table_generator;

  // Slots for the results of the item executions. Its length is the max_element of `_benchmark_item_runner->items()`.
  // with slots staying unused if they are not in `_benchmark_item_runner->items()`. This scheme was chosen since
  // concurrent write access to _results is required.
  std::vector<BenchmarkItemResult> _results;

  nlohmann::json _context;

  std::optional<PerformanceWarningDisabler> _performance_warning_disabler;

  std::chrono::steady_clock::time_point _benchmark_start;
  Duration _total_run_duration{};

  // The atomic uints are modified by other threads when finishing an item, to keep track of when we can
  // let a simulated client schedule the next item, as well as the total number of finished items so far
  std::atomic_uint _currently_running_clients{0};

  // For BenchmarkMode::Shuffled, we count the number of runs executed across all items. This also includes items that
  // were unsuccessful (e.g., because of transaction aborts).
  std::atomic_uint _total_finished_runs{0};

  BenchmarkState _state{Duration{0}};
};

}  // namespace opossum
