#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "concurrentqueue.h"  // The lightweight semaphore uses definitions of concurrentqueue.h.
#include "cxxopts.hpp"
#include "lightweightsemaphore.h"
#include "nlohmann/json.hpp"

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

namespace hyrise {

class SQLPipeline;
struct SQLPipelineMetrics;
class SQLiteWrapper;

// The BenchmarkRunner is the main class for the benchmark framework. It gets initialized by the benchmark binaries
// (e.g., tpch_benchmark.cpp). They then hand over the control to the BenchmarkRunner (inversion of control), which
// calls the supplied table generator, runs and times the benchmark items, and reports the benchmark results.
class BenchmarkRunner : public Noncopyable {
 public:
  // Defines the interval in which the system utilization is collected.
  static constexpr auto SYSTEM_UTILIZATION_TRACKING_INTERVAL = std::chrono::seconds{1};

  BenchmarkRunner(const BenchmarkConfig& config, std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner,
                  std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context);

  void run();

  static cxxopts::Options get_basic_cli_options(const std::string& benchmark_name);

  static nlohmann::json create_context(const BenchmarkConfig& config);

  // If the query execution should be validated, this stores a pointer to the used SQLite instance.
  std::shared_ptr<SQLiteWrapper> sqlite_wrapper;

 private:
  // Run benchmark in BenchmarkMode::Shuffled mode.
  void _benchmark_shuffled();

  // Run benchmark in BenchmarkMode::Ordered mode.
  void _benchmark_ordered();

  // Execute warmup run(s) of a benchmark item. The item's SQL query will be cached.
  void _warmup(const BenchmarkItemID item_id);

  // Schedules a run of the specified for execution. After execution, the result is updated. If the scheduler is
  // disabled, the item is executed immediately.
  void _schedule_item_run(const BenchmarkItemID item_id);

  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json.
  nlohmann::json _create_report() const;

  // Write the @param report to the @param file_name. This is idempotent, i.e., you can call it multiple times and the
  // resulting file will be overwritten. Be aware that writing the file may affect the performance of concurrently
  // running queries.
  static void _write_report_to_file(const std::string& file_name, const nlohmann::json& report);

  // Converts the result of a SQL query into a JSON object.
  static nlohmann::json _sql_to_json(const std::string& sql);

  // Writes the current meta_segments table into the benchmark_segments_log tables. The `moment` parameter can be used
  // to identify a certain point in the benchmark, e.g., when an item is finished in the ordered mode.
  void _snapshot_segment_access_counters(const std::string& moment = "");

  const BenchmarkConfig _config;

  std::unique_ptr<AbstractBenchmarkItemRunner> _benchmark_item_runner;
  std::unique_ptr<AbstractTableGenerator> _table_generator;

  // Slots for the results of the item executions. Its length is the max_element of `_benchmark_item_runner->items()`.
  // with slots staying unused if they are not in `_benchmark_item_runner->items()`. This scheme was chosen since
  // concurrent write access to _results is required.
  std::vector<BenchmarkItemResult> _results;

  nlohmann::json _context;

  std::optional<PerformanceWarningDisabler> _performance_warning_disabler;

  // This is a steady_clock timestamp. steady_clock guarantees that the clock is not adjusted while benchmarking.
  TimePoint _benchmark_start;
  // We need the system_clock here to provide human-readable timestamps relative to the benchmark start for log entries.
  std::chrono::system_clock::time_point _benchmark_wall_clock_start;

  // The atomic uints are modified by other threads when finishing an item, to keep track of when we can
  // let a simulated client schedule the next item, as well as the total number of finished items so far.
  std::atomic_uint32_t _currently_running_clients{0};

  // We schedule as many items simultaneously as we have simulated clients. We use a counting semaphore for this
  // purpose. We initialize it to a maximum value of int32_t::max(), which should be sufficient for reasonable clients
  // counts.
  // We are not using an `std::counting_semaphore` here as the semaphore of libc++ shipped with macOS 14 has wake up
  // issues (see https://reviews.llvm.org/D114119).
  moodycamel::LightweightSemaphore _running_clients_semaphore{0};

  // For BenchmarkMode::Shuffled, we count the number of runs executed across all items. This also includes items that
  // were unsuccessful (e.g., because of transaction aborts).
  std::atomic_uint32_t _total_finished_runs{0};

  BenchmarkState _state{Duration{0}};

  int _snapshot_id{0};

  std::vector<std::string> _loaded_plugins{};
};

}  // namespace hyrise
