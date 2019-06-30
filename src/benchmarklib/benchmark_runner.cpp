#include <json.hpp>

#include <boost/algorithm/string/join.hpp>
#include <boost/range/adaptors.hpp>
#include <fstream>
#include <random>

#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "constant_mappings.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/check_table_equal.hpp"
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
    Topology::use_default_topology(config.cores);
    std::cout << "- Multi-threaded Topology:" << std::endl;
    std::cout << Topology::get();

    // Add NUMA topology information to the context, for processing in the benchmark_multithreaded.py script
    auto numa_cores_per_node = std::vector<size_t>();
    for (const auto& node : Topology::get().nodes()) {
      numa_cores_per_node.push_back(node.cpus.size());
    }
    _context.push_back({"utilized_cores_per_numa_node", numa_cores_per_node});

    const auto scheduler = std::make_shared<NodeQueueScheduler>();
    CurrentScheduler::set(scheduler);
  }

  _table_generator->generate_and_store();

  _benchmark_item_runner->on_tables_loaded();

  if (_config.verify) {
    std::cout << "- Loading tables into SQLite for verification." << std::endl;
    Timer timer;

    // Load the data into SQLite
    sqlite_wrapper = std::make_shared<SQLiteWrapper>();
    for (const auto& [table_name, table] : StorageManager::get().tables()) {
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

  auto benchmark_start = std::chrono::steady_clock::now();

  const auto& items = _benchmark_item_runner->items();
  if (!items.empty()) {
    _results.resize(*std::max_element(items.begin(), items.end()) + 1u);
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
  _total_run_duration = benchmark_end - benchmark_start;

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
      std::cout << "  -> Executed " << _results[item_id].num_iterations.load() << " times" << std::endl;
    }
  }

  // Fail if verification against SQLite was requested and failed
  if (_config.verify) {
    auto any_verification_failed = false;

    for (const auto& item_id : items) {
      const auto& result = _results[item_id];
      Assert(result.verification_passed, "Verification result should have been set");
      any_verification_failed |= !result.verification_passed;
    }

    Assert(!any_verification_failed, "Verification failed");
  }

  if (CurrentScheduler::is_set()) CurrentScheduler::get()->finish();
}

void BenchmarkRunner::_benchmark_shuffled() {
  auto item_ids = _benchmark_item_runner->items();

  auto item_ids_shuffled = std::vector<BenchmarkItemID>{};

  for (const auto& item_id : item_ids) {
    _warmup(item_id);
  }

  // For shuffling the item order
  std::random_device random_device;
  std::mt19937 random_generator(random_device());

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.max_duration};

  while (_state.keep_running() && _total_finished_runs.load(std::memory_order_relaxed) < _config.max_runs) {
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

  // Wait for the rest of the tasks that didn't make it in time - they will not count towards the results
  CurrentScheduler::wait_for_all_tasks();
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

    while (_state.keep_running() && result.num_iterations.load(std::memory_order_relaxed) < _config.max_runs) {
      // We want to only schedule as many items simultaneously as we have simulated clients
      if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
        _schedule_item_run(item_id);
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    _state.set_done();
    result.duration_of_all_runs = _state.benchmark_duration;

    const auto duration_of_all_runs_ns =
        static_cast<float>(std::chrono::duration_cast<std::chrono::nanoseconds>(result.duration_of_all_runs).count());
    const auto duration_seconds = duration_of_all_runs_ns / 1'000'000'000;
    const auto items_per_second = static_cast<float>(result.num_iterations) / duration_seconds;

    if (!_config.verify && !_config.enable_visualization) {
      std::cout << "  -> Executed " << result.num_iterations.load() << " times in " << duration_seconds << " seconds ("
                << items_per_second << " iter/s)" << std::endl;
    }

    // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
    CurrentScheduler::wait_for_all_tasks();
    Assert(_currently_running_clients == 0, "All runs must be finished at this point");
  }
}

void BenchmarkRunner::_schedule_item_run(const BenchmarkItemID item_id) {
  _currently_running_clients++;
  BenchmarkItemResult& result = _results[item_id];

  auto task = std::make_shared<JobTask>(
      [&, item_id]() {
        const auto run_start = std::chrono::steady_clock::now();
        auto [metrics, any_run_verification_failed] = _benchmark_item_runner->execute_item(item_id);  // NOLINT
        const auto run_end = std::chrono::steady_clock::now();

        --_currently_running_clients;
        ++_total_finished_runs;

        // If result.verification_passed was previously unset, set it; otherwise only invalidate it if the run failed.
        result.verification_passed = result.verification_passed.value_or(true) && !any_run_verification_failed;

        if (!_state.is_done()) {  // To prevent items from adding their result after the time is up
          result.num_iterations++;

          result.durations.push_back(run_end - run_start);
          result.metrics.push_back(metrics);
        }
      },
      SchedulePriority::High);

  // No need to check if the benchmark uses the scheduler or not as this method executes tasks immediately if the
  // scheduler is not set.
  CurrentScheduler::schedule_tasks<JobTask>({task});
}

void BenchmarkRunner::_warmup(const BenchmarkItemID item_id) {
  if (_config.warmup_duration == Duration{0}) return;

  const auto& name = _benchmark_item_runner->item_name(item_id);
  BenchmarkItemResult& result = _results[item_id];
  std::cout << "- Warming up for " << name << std::endl;

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.warmup_duration};

  while (_state.keep_running() && result.num_iterations.load(std::memory_order_relaxed) < _config.max_runs) {
    // We want to only schedule as many items simultaneously as we have simulated clients
    if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
      _schedule_item_run(item_id);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

  // Clear the results
  auto empty_result = BenchmarkItemResult{};
  _results[item_id] = std::move(empty_result);

  _state.set_done();

  // Wait for the rest of the tasks that didn't make it in time
  CurrentScheduler::wait_for_all_tasks();
  Assert(_currently_running_clients == 0, "All runs must be finished at this point");
}

void BenchmarkRunner::_create_report(std::ostream& stream) const {
  nlohmann::json benchmarks;

  for (const auto& item_id : _benchmark_item_runner->items()) {
    const auto& name = _benchmark_item_runner->item_name(item_id);
    const auto& result = _results.at(item_id);
    Assert(result.metrics.size() == result.num_iterations,
           "number of iterations and number of iteration durations does not match");

    auto durations_json = nlohmann::json::array();
    for (const auto& duration : result.durations) {
      durations_json.push_back(duration.count());
    }

    // Convert the SQLPipelineMetrics for each run of the BenchmarkItem into JSON
    auto item_metrics_json = nlohmann::json::array();

    for (const auto& item_metric : result.metrics) {
      auto all_pipeline_metrics_json = nlohmann::json::array();

      for (const auto& sql_metric : item_metric) {
        // clang-format off
        auto sql_metric_json = nlohmann::json{
          {"parse_duration", sql_metric.parse_time_nanos.count()},
          {"statements", nlohmann::json::array()}
        };

        for (const auto& sql_statement_metrics : sql_metric.statement_metrics) {
          auto sql_statement_metrics_json = nlohmann::json{
            {"sql_translation_duration", sql_statement_metrics->sql_translation_duration.count()},
            {"optimization_duration", sql_statement_metrics->optimization_duration.count()},
            {"lqp_translation_duration", sql_statement_metrics->lqp_translation_duration.count()},
            {"plan_execution_duration", sql_statement_metrics->plan_execution_duration.count()},
            {"query_plan_cache_hit", sql_statement_metrics->query_plan_cache_hit}
          };

          sql_metric_json["statements"].push_back(sql_statement_metrics_json);
        }
        // clang-format on

        all_pipeline_metrics_json.push_back(sql_metric_json);
      }
      item_metrics_json.push_back(all_pipeline_metrics_json);
    }

    nlohmann::json benchmark{{"name", name},
                             {"durations", durations_json},
                             {"metrics", item_metrics_json},
                             {"iterations", result.num_iterations.load()}};

    if (_config.benchmark_mode == BenchmarkMode::Ordered) {
      // These metrics are not meaningful for permuted / shuffled execution
      const auto duration_of_all_runs_ns =
          static_cast<float>(std::chrono::duration_cast<std::chrono::nanoseconds>(result.duration_of_all_runs).count());
      const auto duration_seconds = duration_of_all_runs_ns / 1'000'000'000;
      const auto items_per_second = static_cast<float>(result.num_iterations) / duration_seconds;
      benchmark["items_per_second"] = items_per_second;
      const auto time_per_item =
          result.num_iterations > 0 ? duration_of_all_runs_ns / result.num_iterations : std::nanf("");
      benchmark["avg_real_time_per_iteration"] = time_per_item;
    }

    benchmarks.push_back(benchmark);
  }

  // Gather information on the (estimated) table size
  auto table_size = size_t{0};
  for (const auto& table_pair : StorageManager::get().tables()) {
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
    ("r,runs", "Maximum number of runs per item", cxxopts::value<size_t>()->default_value("10000")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 100,000", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::DEFAULT_SIZE))) // NOLINT
    ("t,time", "Runtime - per item for Ordered, total for Shuffled", cxxopts::value<size_t>()->default_value("60")) // NOLINT
    ("w,warmup", "Number of seconds that each item is run for warm up", cxxopts::value<size_t>()->default_value("0")) // NOLINT
    ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("m,mode", "Ordered or Shuffled, default is Ordered", cxxopts::value<std::string>()->default_value("Ordered")) // NOLINT
    ("e,encoding", "Specify Chunk encoding as a string or as a JSON config file (for more detailed configuration, see --full_help). String options: " + encoding_strings_option, cxxopts::value<std::string>()->default_value("Dictionary"))  // NOLINT
    ("compression", "Specify vector compression as a string. Options: " + compression_strings_option, cxxopts::value<std::string>()->default_value(""))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cores", "Specify the number of cores used by the scheduler (if active). 0 means all available cores", cxxopts::value<uint>()->default_value("0")) // NOLINT
    ("clients", "Specify how many items should run in parallel if the scheduler is active", cxxopts::value<uint>()->default_value("1")) // NOLINT
    ("visualize", "Create a visualization image of one LQP and PQP for each query, do not properly run the benchmark", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("verify", "Verify each query by comparing it with the SQLite result", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cache_binary_tables", "Cache tables as binary files for faster loading on subsequent runs", cxxopts::value<bool>()->default_value("false")); // NOLINT

  if constexpr (HYRISE_JIT_SUPPORT) {
    cli_options.add_options()
      ("jit", "Enable just-in-time query compilation", cxxopts::value<bool>()->default_value("false")); // NOLINT
  }
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
      {"benchmark_mode", config.benchmark_mode == BenchmarkMode::Ordered ? "Ordered" : "Shuffled"},
      {"max_runs", config.max_runs},
      {"max_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(config.max_duration).count()},
      {"warmup_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(config.warmup_duration).count()},
      {"using_scheduler", config.enable_scheduler},
      {"using_jit", config.enable_jit},
      {"cores", config.cores},
      {"clients", config.clients},
      {"verify", config.verify},
      {"time_unit", "ns"},
      {"GIT-HASH", GIT_HEAD_SHA1 + std::string(GIT_IS_DIRTY ? "-dirty" : "")}};
}

}  // namespace opossum
