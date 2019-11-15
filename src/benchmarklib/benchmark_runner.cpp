#include "benchmark_runner.hpp"

#include <fstream>
#include <random>

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

namespace opossum {

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig& config,
                                 std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner,
                                 std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context)
    : _config(config),
      _benchmark_item_runner(std::move(benchmark_item_runner)),
      _table_generator(std::move(table_generator)),
      _context(context) {
  SQLPipelineBuilder::default_pqp_cache = std::make_shared<SQLPhysicalPlanCache>();
  SQLPipelineBuilder::default_lqp_cache = std::make_shared<SQLLogicalPlanCache>();

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
      sqlite_wrapper->create_table(*table, table_name);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    std::cout << "- All tables loaded into SQLite (" << timer.lap_formatted() << ")" << std::endl;
    _benchmark_item_runner->set_sqlite_wrapper(sqlite_wrapper);
  }
}

void BenchmarkRunner::run() {
  std::cout << "- Starting Benchmark..." << std::endl;

  _benchmark_start = std::chrono::steady_clock::now();

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

  auto benchmark_end = std::chrono::steady_clock::now();
  _total_run_duration = benchmark_end - _benchmark_start;

  // Create report
  if (_config.output_file_path) {
    if (!_config.verify && !_config.enable_visualization) {
      std::ofstream output_file(*_config.output_file_path);
      _create_report(output_file);
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
      Assert(result.verification_passed.load(), "Verification result should have been set");
      any_verification_failed |= !(*result.verification_passed.load());
    }

    Assert(!any_verification_failed, "Verification failed");
  }

  Hyrise::get().scheduler()->finish();
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

    result.duration = _state.benchmark_duration;
    const auto duration_of_all_runs_ns =
        static_cast<float>(std::chrono::duration_cast<std::chrono::nanoseconds>(_state.benchmark_duration).count());
    const auto duration_seconds = duration_of_all_runs_ns / 1'000'000'000.f;
    const auto items_per_second = static_cast<float>(result.successful_runs.size()) / duration_seconds;
    const auto num_successful_runs = result.successful_runs.size();
    const auto duration_per_item =
        num_successful_runs > 0 ? static_cast<float>(duration_seconds) / num_successful_runs : NAN;

    if (!_config.verify && !_config.enable_visualization) {
      std::cout << "  -> Executed " << result.successful_runs.size() << " times in " << duration_seconds << " seconds ("
                << items_per_second << " iter/s, " << duration_per_item << " s/iter)" << std::endl;
      if (!result.unsuccessful_runs.empty()) {
        std::cout << "  -> " << result.unsuccessful_runs.size() << " additional runs failed" << std::endl;
      }
    }

    // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
    Hyrise::get().scheduler()->wait_for_all_tasks();
    Assert(_currently_running_clients == 0, "All runs must be finished at this point");
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
          if (!_config.sql_metrics) metrics.clear();
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
  if (_config.warmup_duration == Duration{0}) return;

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

void BenchmarkRunner::_create_report(std::ostream& stream) const {
  nlohmann::json benchmarks;

  for (const auto& item_id : _benchmark_item_runner->items()) {
    const auto& name = _benchmark_item_runner->item_name(item_id);
    const auto& result = _results.at(item_id);

    const auto runs_to_json = [](auto runs) {
      auto runs_json = nlohmann::json::array();
      for (const auto& run_result : runs) {
        // Convert the SQLPipelineMetrics for each run of the BenchmarkItem into JSON
        auto all_pipeline_metrics_json = nlohmann::json::array();
        // metrics can be empty if _config.sql_metrics is false
        for (const auto& pipeline_metrics : run_result.metrics) {
          auto pipeline_metrics_json = nlohmann::json{{"parse_duration", pipeline_metrics.parse_time_nanos.count()},
                                                      {"statements", nlohmann::json::array()}};

          for (const auto& sql_statement_metrics : pipeline_metrics.statement_metrics) {
            auto sql_statement_metrics_json =
                nlohmann::json{{"sql_translation_duration", sql_statement_metrics->sql_translation_duration.count()},
                               {"optimization_duration", sql_statement_metrics->optimization_duration.count()},
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

    nlohmann::json benchmark{
        {"name", name},
        {"duration", std::chrono::duration_cast<std::chrono::nanoseconds>(result.duration).count()},
        {"successful_runs", runs_to_json(result.successful_runs)},
        {"unsuccessful_runs", runs_to_json(result.unsuccessful_runs)},
        {"iterations", result.successful_runs.size()}};

    // For ordered benchmarks, report the time that this individual item ran. For shuffled benchmarks, return the
    // duration of the entire benchmark. This means that items_per_second of ordered and shuffled runs are not
    // comparable.
    const auto reported_item_duration =
        _config.benchmark_mode == BenchmarkMode::Shuffled ? _total_run_duration : result.duration;
    const auto reported_item_duration_ns =
        static_cast<float>(std::chrono::duration_cast<std::chrono::nanoseconds>(reported_item_duration).count());
    const auto duration_seconds = reported_item_duration_ns / 1'000'000'000.f;
    const auto items_per_second = static_cast<float>(result.successful_runs.size()) / duration_seconds;

    // The field items_per_second is relied upon by a number of visualization scripts. Carefully consider if you really
    // want to touch this and potentially break the comparability across commits.
    benchmark["items_per_second"] = items_per_second;
    const auto time_per_item =
        !result.successful_runs.empty() ? reported_item_duration_ns / result.successful_runs.size() : std::nanf("");
    benchmark["avg_real_time_per_iteration"] = time_per_item;

    benchmarks.push_back(benchmark);
  }

  // Gather information on the (estimated) table size
  auto table_size = size_t{0};
  for (const auto& table_pair : Hyrise::get().storage_manager.tables()) {
    table_size += table_pair.second->estimate_memory_usage();
  }

  nlohmann::json summary{
      {"table_size_in_bytes", table_size},
      {"total_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(_total_run_duration).count()}};

  nlohmann::json report{{"context", _context},
                        {"benchmarks", benchmarks},
                        {"summary", summary},
                        {"table_generation", _table_generator->metrics}};

  stream << std::setw(2) << report << std::endl;
}

cxxopts::Options BenchmarkRunner::get_basic_cli_options(const std::string& benchmark_name) {
  cxxopts::Options cli_options{benchmark_name};

  // Create a comma separated strings with the encoding and compression options
  const auto get_first = boost::adaptors::transformed([](auto it) { return it.first; });
  const auto encoding_strings_option = boost::algorithm::join(encoding_type_to_string.right | get_first, ", ");
  const auto compression_strings_option =
      boost::algorithm::join(vector_compression_type_to_string.right | get_first, ", ");

  // If you add a new option here, make sure to edit CLIConfigParser::basic_cli_options_to_json() so it contains the
  // newest options. Sadly, there is no way to to get all option keys to do this automatically.
  // clang-format off
  cli_options.add_options()
    ("help", "print a summary of CLI options")
    ("full_help", "print more detailed information about configuration options")
    ("r,runs", "Maximum number of runs per item, negative values mean infinity", cxxopts::value<int64_t>()->default_value("-1")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 100,000", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::DEFAULT_SIZE))) // NOLINT
    ("t,time", "Runtime - per item for Ordered, total for Shuffled", cxxopts::value<size_t>()->default_value("60")) // NOLINT
    ("w,warmup", "Number of seconds that each item is run for warm up", cxxopts::value<size_t>()->default_value("0")) // NOLINT
    ("o,output", "JSON file to output results to, don't specify for stdout", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("m,mode", "Ordered or Shuffled, default is Ordered", cxxopts::value<std::string>()->default_value("Ordered")) // NOLINT
    ("e,encoding", "Specify Chunk encoding as a string or as a JSON config file (for more detailed configuration, see --full_help). String options: " + encoding_strings_option, cxxopts::value<std::string>()->default_value("Dictionary"))  // NOLINT
    ("compression", "Specify vector compression as a string. Options: " + compression_strings_option, cxxopts::value<std::string>()->default_value(""))  // NOLINT
    ("indexes", "Create indexes (where defined by benchmark)", cxxopts::value<bool>()->default_value("false"))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cores", "Specify the number of cores used by the scheduler (if active). 0 means all available cores", cxxopts::value<uint>()->default_value("0")) // NOLINT
    ("clients", "Specify how many items should run in parallel if the scheduler is active", cxxopts::value<uint>()->default_value("1")) // NOLINT
    ("visualize", "Create a visualization image of one LQP and PQP for each query, do not properly run the benchmark", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("verify", "Verify each query by comparing it with the SQLite result", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cache_binary_tables", "Cache tables as binary files for faster loading on subsequent runs", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("sql_metrics", "Track SQL metrics (parse time etc.) for each SQL query and add it to the output JSON (see -o)", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  return cli_options;
}

nlohmann::json BenchmarkRunner::create_context(const BenchmarkConfig& config) {
  // Generate YY-MM-DD hh:mm::ss
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
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

  return nlohmann::json{
      {"date", timestamp_stream.str()},
      {"chunk_size", config.chunk_size},
      {"compiler", compiler.str()},
      {"build_type", HYRISE_DEBUG ? "debug" : "release"},
      {"encoding", config.encoding_config.to_json()},
      {"indexes", config.indexes},
      {"benchmark_mode", config.benchmark_mode == BenchmarkMode::Ordered ? "Ordered" : "Shuffled"},
      {"max_runs", config.max_runs},
      {"max_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(config.max_duration).count()},
      {"warmup_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(config.warmup_duration).count()},
      {"using_scheduler", config.enable_scheduler},
      {"cores", config.cores},
      {"clients", config.clients},
      {"verify", config.verify},
      {"time_unit", "ns"},
      {"GIT-HASH", GIT_HEAD_SHA1 + std::string(GIT_IS_DIRTY ? "-dirty" : "")}};
}

}  // namespace opossum
