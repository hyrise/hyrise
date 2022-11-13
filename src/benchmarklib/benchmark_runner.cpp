#include "benchmark_runner.hpp"

#include <chrono>
#include <fstream>
#include <random>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/stats.hpp>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptors.hpp>
#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "constant_mappings.hpp"
#include "hyrise.hpp"
#include "scheduler/job_task.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/format_duration.hpp"
#include "utils/sqlite_wrapper.hpp"
#include "utils/timer.hpp"
#include "version.hpp"

namespace hyrise {

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig& config,
                                 std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner,
                                 std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context)
    : _config(config),
      _benchmark_item_runner(std::move(benchmark_item_runner)),
      _table_generator(std::move(table_generator)),
      _context(context) {
  Hyrise::get().default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  Hyrise::get().default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

  // Initialise the scheduler if the benchmark was requested to run multi-threaded
  if (config.enable_scheduler) {
    Hyrise::get().topology.use_default_topology(config.cores);
    std::cout << "- Multi-threaded Topology:" << std::endl;
    std::cout << Hyrise::get().topology;

    // Add NUMA topology information to the context, for processing in the benchmark_multithreaded.py script
    auto numa_cores_per_node = std::vector<size_t>();
    for (const auto& node : Hyrise::get().topology.nodes()) {
      numa_cores_per_node.push_back(node.cpus.size());
    }
    _context.push_back({"utilized_cores_per_numa_node", numa_cores_per_node});

    const auto scheduler = std::make_shared<NodeQueueScheduler>();
    Hyrise::get().set_scheduler(scheduler);
  }

  _table_generator->generate_and_store();

  _benchmark_item_runner->on_tables_loaded();

  // SQLite data is only loaded if the dedicated result set is not complete, i.e,
  // items exist for which no dedicated result could be loaded.
  if (_config.verify && _benchmark_item_runner->has_item_without_dedicated_result()) {
    std::cout << "- Loading tables into SQLite for verification." << std::endl;
    Timer timer;

    // Load the data into SQLite
    sqlite_wrapper = std::make_shared<SQLiteWrapper>();
    for (const auto& [table_name, table] : Hyrise::get().storage_manager.tables()) {
      std::cout << "-  Loading '" << table_name << "' into SQLite " << std::flush;
      Timer per_table_timer;
      sqlite_wrapper->create_sqlite_table(*table, table_name);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    std::cout << "- All tables loaded into SQLite (" << timer.lap_formatted() << ")" << std::endl;
    _benchmark_item_runner->set_sqlite_wrapper(sqlite_wrapper);
  }
}

void BenchmarkRunner::run() {
  std::cout << "- Starting Benchmark..." << std::endl;

  _benchmark_start = std::chrono::steady_clock::now();
  _benchmark_wall_clock_start = std::chrono::system_clock::now();

  auto track_system_utilization = std::atomic_bool{_config.metrics};
  auto system_utilization_tracker = std::thread{[&] {
    if (!track_system_utilization) {
      return;
    }

    // Start tracking the system utilization
    SQLPipelineBuilder{
        "CREATE TABLE benchmark_system_utilization_log AS SELECT CAST(0 as LONG) AS \"timestamp\", * FROM "
        "meta_system_utilization"}
        .create_pipeline()
        .get_result_table();

    while (track_system_utilization) {
      const auto timestamp = std::chrono::nanoseconds{std::chrono::steady_clock::now() - _benchmark_start}.count();

      auto sql_builder = std::stringstream{};
      sql_builder << "INSERT INTO benchmark_system_utilization_log SELECT CAST(" << timestamp
                  << "as LONG), * FROM meta_system_utilization";

      SQLPipelineBuilder{sql_builder.str()}.create_pipeline().get_result_table();

      std::this_thread::sleep_for(SYSTEM_UTILIZATION_TRACKING_INTERVAL);
    }
  }};

  if (_config.metrics) {
    // Create a table for the segment access counter log
    SQLPipelineBuilder{
        "CREATE TABLE benchmark_segments_log AS SELECT 0 AS snapshot_id, 'init' AS moment, * FROM meta_segments"}
        .create_pipeline()
        .get_result_table();
  }

  // Retrieve the items to be executed and prepare the result vector
  const auto& items = _benchmark_item_runner->items();
  if (!items.empty()) {
    _results = std::vector<BenchmarkItemResult>{*std::max_element(items.begin(), items.end()) + 1u};
  }

  switch (_config.benchmark_mode) {
    case BenchmarkMode::Ordered: {
      _benchmark_ordered();
      break;
    }
    case BenchmarkMode::Shuffled: {
      _benchmark_shuffled();
      break;
    }
  }

  // Create report
  if (_config.output_file_path) {
    if (!_config.verify && !_config.enable_visualization) {
      write_report_to_file();
    } else {
      std::cout << "- Not writing JSON result as either verification or visualization are activated." << std::endl;
      std::cout << "  These options make the results meaningless." << std::endl;
    }
  }

  // For the Ordered mode, results have already been printed to the console
  if (_config.benchmark_mode == BenchmarkMode::Shuffled && !_config.verify && !_config.enable_visualization) {
    for (const auto& item_id : items) {
      std::cout << "- Results for " << _benchmark_item_runner->item_name(item_id) << std::endl;
      std::cout << "  -> Executed " << _results[item_id].successful_runs.size() << " times" << std::endl;
      if (!_results[item_id].unsuccessful_runs.empty()) {
        std::cout << "  -> " << _results[item_id].unsuccessful_runs.size() << " additional runs failed" << std::endl;
      }
    }
  }

  // Fail if verification against SQLite was requested and failed
  if (_config.verify) {
    auto any_verification_failed = false;

    for (const auto& item_id : items) {
      const auto& result = _results[item_id];
      if (result.successful_runs.empty()) {
        continue;
      }
      Assert(result.verification_passed.load(), "Verification result should have been set");
      any_verification_failed |= !(*result.verification_passed.load());
    }

    Assert(!any_verification_failed, "Verification failed");
  }

  if (Hyrise::get().scheduler()) {
    Hyrise::get().scheduler()->finish();
    Hyrise::get().set_scheduler(std::make_shared<ImmediateExecutionScheduler>());
  }

  // Stop the thread that tracks the system utilization
  track_system_utilization = false;
  system_utilization_tracker.join();
}

void BenchmarkRunner::_benchmark_shuffled() {
  auto item_ids = _benchmark_item_runner->items();

  if (const auto& weights = _benchmark_item_runner->weights(); !weights.empty()) {
    auto item_ids_weighted = std::vector<BenchmarkItemID>{};
    for (const auto& selected_item_id : item_ids) {
      const auto item_weight = weights.at(selected_item_id);
      item_ids_weighted.resize(item_ids_weighted.size() + item_weight, selected_item_id);
    }
    item_ids = item_ids_weighted;
  }

  auto item_ids_shuffled = std::vector<BenchmarkItemID>{};

  for (const auto& item_id : item_ids) {
    _warmup(item_id);
  }

  // For shuffling the item order
  std::random_device random_device;
  std::mt19937 random_generator(random_device());

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.max_duration};

  while (_state.keep_running() && (_config.max_runs < 0 || _total_finished_runs.load(std::memory_order_relaxed) <
                                                               static_cast<size_t>(_config.max_runs))) {
    // We want to only schedule as many items simultaneously as we have simulated clients
    if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
      if (item_ids_shuffled.empty()) {
        item_ids_shuffled = item_ids;
        std::shuffle(item_ids_shuffled.begin(), item_ids_shuffled.end(), random_generator);
      }

      const auto item_id = item_ids_shuffled.back();
      item_ids_shuffled.pop_back();

      _schedule_item_run(item_id);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  _state.set_done();

  for (auto& result : _results) {
    // As the execution of benchmark items is intermingled, we use the total duration for all items
    result.duration = _state.benchmark_duration;
  }

  // Wait for the rest of the tasks that didn't make it in time - they will not count towards the results
  Hyrise::get().scheduler()->wait_for_all_tasks();
  Assert(_currently_running_clients == 0, "All runs must be finished at this point");

  _snapshot_segment_access_counters("End of Benchmark");
}

void BenchmarkRunner::_benchmark_ordered() {
  for (const auto& item_id : _benchmark_item_runner->items()) {
    _warmup(item_id);

    const auto& name = _benchmark_item_runner->item_name(item_id);
    std::cout << "- Benchmarking " << name << std::endl;

    auto& result = _results[item_id];

    Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

    _state = BenchmarkState{_config.max_duration};

    while (_state.keep_running() &&
           (_config.max_runs < 0 || (result.successful_runs.size() + result.unsuccessful_runs.size()) <
                                        static_cast<size_t>(_config.max_runs))) {
      // We want to only schedule as many items simultaneously as we have simulated clients
      if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
        _schedule_item_run(item_id);
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    _state.set_done();

    // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
    if (_currently_running_clients > 0) {
      std::cout << "  -> Waiting for clients that are still running" << std::endl;
    }
    Hyrise::get().scheduler()->wait_for_all_tasks();
    Assert(_currently_running_clients == 0, "All runs must be finished at this point");

    result.duration = _state.benchmark_duration;
    // chrono::seconds uses an integer precision duration type, but we need a floating-point value.
    const auto duration_seconds = std::chrono::duration<double>{_state.benchmark_duration}.count();
    const auto items_per_second = static_cast<double>(result.successful_runs.size()) / duration_seconds;

    // Compute mean by using accumulators
    boost::accumulators::accumulator_set<double, boost::accumulators::stats<boost::accumulators::tag::mean>>
        accumulator;
    for (const auto& entry : result.successful_runs) {
      // For readability and to be consistent with compare_benchmarks.py, query latencies should be in milliseconds.
      // chrono::milliseconds uses an integer precision duration type, but we need a floating-point value.
      accumulator(std::chrono::duration<double, std::milli>{entry.duration}.count());
    }
    const auto mean_in_milliseconds = boost::accumulators::mean(accumulator);

    if (!_config.verify && !_config.enable_visualization) {
      std::cout << "  -> Executed " << result.successful_runs.size() << " times in " << duration_seconds
                << " seconds (Latency: " << mean_in_milliseconds << " ms/iter, Throughput: " << items_per_second
                << " iter/s)" << std::endl;
      if (!result.unsuccessful_runs.empty()) {
        std::cout << "  -> " << result.unsuccessful_runs.size() << " additional runs failed" << std::endl;
      }
    }

    // Taking the snapshot at this point means that both warmup runs and runs that finish after the deadline are taken
    // into account, too. In light of the significant amount of data added by the snapshots to the JSON file and the
    // unclear advantage of excluding those runs, we only take one snapshot here.
    _snapshot_segment_access_counters(name);
  }
}

void BenchmarkRunner::_schedule_item_run(const BenchmarkItemID item_id) {
  _currently_running_clients++;
  BenchmarkItemResult& result = _results[item_id];

  auto task = std::make_shared<JobTask>(
      [&, item_id]() {
        const auto run_start = std::chrono::steady_clock::now();
        auto [success, metrics, any_run_verification_failed] = _benchmark_item_runner->execute_item(item_id);
        const auto run_end = std::chrono::steady_clock::now();

        --_currently_running_clients;
        ++_total_finished_runs;

        // If result.verification_passed was previously unset, set it; otherwise only invalidate it if the run failed.
        result.verification_passed = result.verification_passed.load().value_or(true) && !any_run_verification_failed;

        if (!_state.is_done()) {  // To prevent items from adding their result after the time is up
          if (!_config.metrics) {
            metrics.clear();
          }
          const auto item_result =
              BenchmarkItemRunResult{run_start - _benchmark_start, run_end - run_start, std::move(metrics)};
          if (success) {
            result.successful_runs.push_back(item_result);
          } else {
            result.unsuccessful_runs.push_back(item_result);
          }
        }
      },
      SchedulePriority::High);

  task->schedule();
}

void BenchmarkRunner::_warmup(const BenchmarkItemID item_id) {
  if (_config.warmup_duration == Duration{0}) {
    return;
  }

  const auto& name = _benchmark_item_runner->item_name(item_id);
  std::cout << "- Warming up for " << name << std::endl;

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.warmup_duration};

  while (_state.keep_running()) {
    // We want to only schedule as many items simultaneously as we have simulated clients
    if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
      _schedule_item_run(item_id);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Clear the results
  _results[item_id].successful_runs = {};
  _results[item_id].unsuccessful_runs = {};
  _results[item_id].duration = {};

  _state.set_done();

  // Wait for the rest of the tasks that didn't make it in time
  Hyrise::get().scheduler()->wait_for_all_tasks();
  Assert(_currently_running_clients == 0, "All runs must be finished at this point");
}

void BenchmarkRunner::write_report_to_file() const {
  const std::chrono::nanoseconds total_duration = std::chrono::steady_clock::now() - _benchmark_start;

  nlohmann::json benchmarks;

  for (const auto& item_id : _benchmark_item_runner->items()) {
    const auto& name = _benchmark_item_runner->item_name(item_id);
    const auto& result = _results.at(item_id);

    const auto runs_to_json = [](auto runs) {
      auto runs_json = nlohmann::json::array();
      for (const auto& run_result : runs) {
        // Convert the SQLPipelineMetrics for each run of the BenchmarkItem into JSON
        auto all_pipeline_metrics_json = nlohmann::json::array();
        // metrics can be empty if _config.metrics is false
        for (const auto& pipeline_metrics : run_result.metrics) {
          auto pipeline_metrics_json = nlohmann::json{{"parse_duration", pipeline_metrics.parse_time_nanos.count()},
                                                      {"statements", nlohmann::json::array()}};

          for (const auto& sql_statement_metrics : pipeline_metrics.statement_metrics) {
            nlohmann::json rule_metrics_json;
            for (const auto& rule_duration : sql_statement_metrics->optimizer_rule_durations) {
              rule_metrics_json[rule_duration.rule_name] += rule_duration.duration.count();
            }

            auto sql_statement_metrics_json =
                nlohmann::json{{"sql_translation_duration", sql_statement_metrics->sql_translation_duration.count()},
                               {"optimization_duration", sql_statement_metrics->optimization_duration.count()},
                               {"optimizer_rule_durations", rule_metrics_json},
                               {"lqp_translation_duration", sql_statement_metrics->lqp_translation_duration.count()},
                               {"plan_execution_duration", sql_statement_metrics->plan_execution_duration.count()},
                               {"query_plan_cache_hit", sql_statement_metrics->query_plan_cache_hit}};

            pipeline_metrics_json["statements"].push_back(sql_statement_metrics_json);
          }

          all_pipeline_metrics_json.push_back(pipeline_metrics_json);
        }

        runs_json.push_back(nlohmann::json{{"begin", run_result.begin.count()},
                                           {"duration", run_result.duration.count()},
                                           {"metrics", all_pipeline_metrics_json}});
      }
      return runs_json;
    };

    nlohmann::json benchmark{{"name", name},
                             {"duration", result.duration.count()},
                             {"successful_runs", runs_to_json(result.successful_runs)},
                             {"unsuccessful_runs", runs_to_json(result.unsuccessful_runs)}};

    // For ordered benchmarks, report the time that this individual item ran. For shuffled benchmarks, return the
    // duration of the entire benchmark. This means that items_per_second of ordered and shuffled runs are not
    // comparable.
    const auto reported_item_duration =
        _config.benchmark_mode == BenchmarkMode::Shuffled ? total_duration : result.duration;
    // chrono::seconds uses an integer precision duration type, but we need a floating-point value.
    const auto duration_seconds = std::chrono::duration<double>(reported_item_duration).count();
    const auto items_per_second =
        duration_seconds > 0 ? (static_cast<double>(result.successful_runs.size()) / duration_seconds) : 0;

    // The field items_per_second is relied upon by a number of visualization scripts. Carefully consider if you really
    // want to touch this and potentially break the comparability across commits. Note that items_per_second only
    // includes successful iterations.
    benchmark["items_per_second"] = items_per_second;

    benchmarks.push_back(benchmark);
  }

  // Gather information on the table size
  auto table_size = size_t{0};
  for (const auto& [_, table] : Hyrise::get().storage_manager.tables()) {
    table_size += table->memory_usage(MemoryUsageCalculationMode::Full);
  }

  nlohmann::json summary{{"table_size_in_bytes", table_size}, {"total_duration", total_duration.count()}};

  // To get timestamps relative to the benchmark start, we substract the benchmark start timepoint.
  // We have to use system_clock here, as the LogManager uses it to provide human-readable timestamps.
  // Because the system_clock can be readjusted anytime, the timestamps could be slightly out of line.
  const auto benchmark_start_ns = std::chrono::nanoseconds{_benchmark_wall_clock_start.time_since_epoch()}.count();
  auto log_json = _sql_to_json(std::string{"SELECT \"timestamp\" - "} + std::to_string(benchmark_start_ns) +
                               " AS \"timestamp\", log_level, reporter, message FROM meta_log");

  nlohmann::json report{{"context", _context},
                        {"benchmarks", std::move(benchmarks)},
                        {"log", std::move(log_json)},
                        {"summary", std::move(summary)},
                        {"table_generation", _table_generator->metrics}};

  // Add information that was temporarily stored in the `benchmark_...` tables during the benchmark execution
  if (Hyrise::get().storage_manager.has_table("benchmark_system_utilization_log")) {
    report["system_utilization"] = _sql_to_json("SELECT * FROM benchmark_system_utilization_log");
  }

  if (Hyrise::get().storage_manager.has_table("benchmark_segments_log")) {
    report["segments"] = _sql_to_json("SELECT * FROM benchmark_segments_log");
  }

  // Write the output file
  std::ofstream{_config.output_file_path.value()} << std::setw(2) << report << std::endl;
}

cxxopts::Options BenchmarkRunner::get_basic_cli_options(const std::string& benchmark_name) {
  cxxopts::Options cli_options{benchmark_name};

  // Create a comma separated strings with the encoding and compression options
  const auto get_first = boost::adaptors::transformed([](auto it) { return it.first; });
  const auto encoding_strings_option = boost::algorithm::join(encoding_type_to_string.right | get_first, ", ");
  const auto compression_strings_option =
      boost::algorithm::join(vector_compression_type_to_string.right | get_first, ", ");

  // Make TPC-C run in shuffled mode. While it can also run in ordered mode, it would run out of orders to fulfill at
  // some point. The way this is solved here is not really nice, but as the TPC-C benchmark binary has just a main
  // method and not a class, retrieving this default value properly would require some major refactoring of how
  // benchmarks interact with the BenchmarkRunner. At this moment, that does not seem to be worth the effort.
  const auto* const default_mode = (benchmark_name == "TPC-C Benchmark" ? "Shuffled" : "Ordered");

  // clang-format off
  cli_options.add_options()
    ("help", "print a summary of CLI options")
    ("full_help", "print more detailed information about configuration options")
    ("r,runs", "Maximum number of runs per item, negative values mean infinity", cxxopts::value<int64_t>()->default_value("-1")) // NOLINT
    ("c,chunk_size", "Chunk size", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::DEFAULT_SIZE))) // NOLINT
    ("t,time", "Runtime - per item for Ordered, total for Shuffled", cxxopts::value<uint64_t>()->default_value("60")) // NOLINT
    ("w,warmup", "Number of seconds that each item is run for warm up", cxxopts::value<uint64_t>()->default_value("0")) // NOLINT
    ("o,output", "JSON file to output results to, don't specify for stdout", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("m,mode", "Ordered or Shuffled", cxxopts::value<std::string>()->default_value(default_mode)) // NOLINT
    ("e,encoding", "Specify Chunk encoding as a string or as a JSON config file (for more detailed configuration, see --full_help). String options: " + encoding_strings_option, cxxopts::value<std::string>()->default_value("Dictionary"))  // NOLINT
    ("compression", "Specify vector compression as a string. Options: " + compression_strings_option, cxxopts::value<std::string>()->default_value(""))  // NOLINT
    ("indexes", "Create indexes (where defined by benchmark)", cxxopts::value<bool>()->default_value("false"))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cores", "Specify the number of cores used by the scheduler (if active). 0 means all available cores", cxxopts::value<uint32_t>()->default_value("0")) // NOLINT
    ("clients", "Specify how many items should run in parallel if the scheduler is active", cxxopts::value<uint32_t>()->default_value("1")) // NOLINT
    ("visualize", "Create a visualization image of one LQP and PQP for each query, do not properly run the benchmark", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("verify", "Verify each query by comparing it with the SQLite result", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("dont_cache_binary_tables", "Do not cache tables as binary files for faster loading on subsequent runs", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("metrics", "Track more metrics (steps in SQL pipeline, system utilization, etc.) and add them to the output JSON (see -o)", cxxopts::value<bool>()->default_value("false")) // NOLINT
    // This option is only advised when the underlying system's memory capacity is overleaded by the preparation phase.
    ("data_preparation_cores", "Specify the number of cores used by the scheduler for data preparation, i.e., sorting and encoding tables and generating table statistics. 0 means all available cores.", cxxopts::value<uint32_t>()->default_value("0")); // NOLINT
  // clang-format on

  return cli_options;
}

nlohmann::json BenchmarkRunner::create_context(const BenchmarkConfig& config) {
  // Generate YY-MM-DD hh:mm::ss
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);  // NOLINT(concurrency-mt-unsafe) - not called in parallel
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  // clang-format off
  std::stringstream compiler;
  #if defined(__clang__)
    compiler << "clang " << __clang_major__ << "." << __clang_minor__ << "." << __clang_patchlevel__;
  #elif defined(__GNUC__)
    compiler << "gcc " << __GNUC__ << "." << __GNUC_MINOR__;
  #else
    compiler << "unknown";
  #endif
  // clang-format on

  return nlohmann::json{{"date", timestamp_stream.str()},
                        {"chunk_size", static_cast<ChunkOffset::base_type>(config.chunk_size)},
                        {"compiler", compiler.str()},
                        {"build_type", HYRISE_DEBUG ? "debug" : "release"},
                        {"encoding", config.encoding_config.to_json()},
                        {"indexes", config.indexes},
                        {"benchmark_mode", magic_enum::enum_name(config.benchmark_mode)},
                        {"max_runs", config.max_runs},
                        {"max_duration", config.max_duration.count()},
                        {"warmup_duration", config.warmup_duration.count()},
                        {"using_scheduler", config.enable_scheduler},
                        {"cores", config.cores},
                        {"clients", config.clients},
                        {"data_preparation_cores", config.data_preparation_cores},
                        {"verify", config.verify},
                        {"time_unit", "ns"},
                        {"GIT-HASH", GIT_HEAD_SHA1 + std::string(GIT_IS_DIRTY ? "-dirty" : "")}};
}

nlohmann::json BenchmarkRunner::_sql_to_json(const std::string& sql) {
  auto pipeline = SQLPipelineBuilder{sql}.create_pipeline();
  const auto& [pipeline_status, table] = pipeline.get_result_table();
  Assert(pipeline_status == SQLPipelineStatus::Success, "_sql_to_json failed");

  auto output = nlohmann::json::array();
  for (auto row_nr = uint64_t{0}; row_nr < table->row_count(); ++row_nr) {
    auto row = table->get_row(row_nr);
    auto entry = nlohmann::json{};

    for (auto column_id = ColumnID{0}; column_id < table->column_count(); ++column_id) {
      boost::apply_visitor(
          // table=table needed because of https://stackoverflow.com/questions/46114214/
          [&, table = table](const auto value) {
            if constexpr (!std::is_same_v<std::decay_t<decltype(value)>, NullValue>) {
              entry[table->column_name(column_id)] = value;
            } else {
              entry[table->column_name(column_id)] = nullptr;
            }
          },
          row[column_id]);
    }

    output.emplace_back(std::move(entry));
  }

  return output;
}

void BenchmarkRunner::_snapshot_segment_access_counters(const std::string& moment) {
  if (!_config.metrics) {
    return;
  }

  auto moment_or_timestamp = moment;
  if (moment_or_timestamp.empty()) {
    const auto timestamp = std::chrono::nanoseconds{std::chrono::steady_clock::now() - _benchmark_start}.count();
    moment_or_timestamp = std::to_string(timestamp);
  }

  ++_snapshot_id;

  auto sql_builder = std::stringstream{};
  sql_builder << "INSERT INTO benchmark_segments_log SELECT " << _snapshot_id << ", '" << moment_or_timestamp + "'"
              << ", * FROM meta_segments WHERE table_name NOT LIKE 'benchmark%'";

  SQLPipelineBuilder{sql_builder.str()}.create_pipeline().get_result_table();
}

}  // namespace hyrise
