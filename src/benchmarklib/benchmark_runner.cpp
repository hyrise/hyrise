#include <json.hpp>

#include <boost/range/adaptors.hpp>
#include <random>

#include "cxxopts.hpp"

#include "benchmark_config.hpp"
#include "benchmark_runner.hpp"
#include "constant_mappings.hpp"
#include "logical_query_plan/jit_aware_lqp_translator.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"
#include "utils/check_table_equal.hpp"
#include "utils/format_duration.hpp"
#include "utils/sqlite_wrapper.hpp"
#include "utils/timer.hpp"
#include "version.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/pqp_visualizer.hpp"

namespace opossum {

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig& config,
                                 std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner,
                                 std::unique_ptr<AbstractTableGenerator> table_generator, const nlohmann::json& context)
    : _config(config),
      _benchmark_item_runner(std::move(benchmark_item_runner)),
      _table_generator(std::move(table_generator)),
      _context(context) {
  // Initialise the scheduler if the benchmark was requested to run multi-threaded
  if (config.enable_scheduler) {
    Topology::use_default_topology(config.cores);
    std::cout << "- Multi-threaded Topology:" << std::endl;
    Topology::get().print(std::cout, 2);

    // Add NUMA topology information to the context, for processing in the benchmark_multithreaded.py script
    auto numa_cores_per_node = std::vector<size_t>();
    for (auto node : Topology::get().nodes()) {
      numa_cores_per_node.push_back(node.cpus.size());
    }
    _context.push_back({"utilized_cores_per_numa_node", numa_cores_per_node});

    const auto scheduler = std::make_shared<NodeQueueScheduler>();
    CurrentScheduler::set(scheduler);
  }
}

BenchmarkRunner::~BenchmarkRunner() {
  if (CurrentScheduler::is_set()) {
    CurrentScheduler::get()->finish();
  }
}

void BenchmarkRunner::run() {
  _table_generator->generate_and_store();

  // Visualize query plans
  if (_config.enable_visualization) {
    // TODO fix
    // for (auto item_id = BenchmarkItemID{0}; item_id < _query_plans.size(); ++item_id) {
    //   const auto& lqps = _query_plans[item_id].lqps;
    //   const auto& pqps = _query_plans[item_id].pqps;

    //   if (lqps.empty()) continue;

    //   auto name = _benchmark_item_runner->item_name(item_id);
    //   boost::replace_all(name, " ", "_");

    //   GraphvizConfig graphviz_config;
    //   graphviz_config.format = "svg";

    //   for (auto lqp_idx = size_t{0}; lqp_idx < lqps.size(); ++lqp_idx) {
    //     const auto file_prefix = name + "-LQP-" + std::to_string(lqp_idx);
    //     LQPVisualizer{graphviz_config, {}, {}, {}}.visualize({lqps[lqp_idx]}, file_prefix + ".dot",
    //                                                          file_prefix + ".svg");
    //   }
    //   for (auto pqp_idx = size_t{0}; pqp_idx < pqps.size(); ++pqp_idx) {
    //     const auto file_prefix = name + "-PQP-" + std::to_string(pqp_idx);
    //     PQPVisualizer{graphviz_config, {}, {}, {}}.visualize({pqps[pqp_idx]}, file_prefix + ".dot",
    //                                                          file_prefix + ".svg");
    //   }
    // }
    // return;
  }

  if (_config.verify) {
    std::cout << "- Loading tables into SQLite for verification." << std::endl;
    Timer timer;

    // Load the data into SQLite
    _sqlite_wrapper = std::make_shared<SQLiteWrapper>();
    for (const auto& [table_name, table] : StorageManager::get().tables()) {
      std::cout << "-  Loading '" << table_name << "' into SQLite " << std::flush;
      Timer per_table_timer;
      _sqlite_wrapper->create_table(*table, table_name);
      std::cout << "(" << per_table_timer.lap_formatted() << ")" << std::endl;
    }
    std::cout << "- All tables loaded into SQLite (" << timer.lap_formatted() << ")" << std::endl;
    _benchmark_item_runner->set_sqlite_wrapper(_sqlite_wrapper);
  }

  // Now run the actual benchmark
  std::cout << "- Starting Benchmark..." << std::endl;

  const auto available_item_count = _benchmark_item_runner->available_item_count();
  _results.resize(available_item_count);

  auto benchmark_start = std::chrono::steady_clock::now();

  // Run the queries in the selected mode
  switch (_config.benchmark_mode) {
    case BenchmarkMode::IndividualQueries: {
      _benchmark_individual_queries();
      break;
    }
    case BenchmarkMode::PermutedQuerySet: {
      // TODO Print results here
      _benchmark_permuted_query_set();
      break;
    }
  }

  auto benchmark_end = std::chrono::steady_clock::now();
  _total_run_duration = benchmark_end - benchmark_start;

  // Create report
  if (_config.output_file_path) {
    std::ofstream output_file(*_config.output_file_path);
    _create_report(output_file);
  }

  // Fail if verification against SQLite was requested and failed
  if (_config.verify) {
    auto any_verification_failed = false;

    for (const auto& selected_item_id : _benchmark_item_runner->selected_items()) {
      const auto& result = _results[selected_item_id];
      Assert(result.verification_passed, "Verification result should have been set");
      any_verification_failed |= !result.verification_passed;
    }

    Assert(!any_verification_failed, "Verification failed");
  }
}

void BenchmarkRunner::_benchmark_permuted_query_set() {
  auto item_ids = _benchmark_item_runner->selected_items();

  auto item_ids_shuffled = std::vector<BenchmarkItemID>{};

  for (const auto& item_id : item_ids) {
    _warmup_query(item_id);
  }

  // For shuffling the query order
  std::random_device random_device;
  std::mt19937 random_generator(random_device());

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.max_duration};

  while (_state.keep_running() && _total_finished_runs.load(std::memory_order_relaxed) < _config.max_num_query_runs) {
    // We want to only schedule as many query sets simultaneously as we have simulated clients
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

  // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
  CurrentScheduler::wait_for_all_tasks();
  Assert(_currently_running_clients == 0, "All query set runs must be finished at this point");
}

void BenchmarkRunner::_benchmark_individual_queries() {
  for (const auto& item_id : _benchmark_item_runner->selected_items()) {
    _warmup_query(item_id);

    const auto& name = _benchmark_item_runner->item_name(item_id);
    std::cout << "- Benchmarking Query " << name << std::endl;

    auto& result = _results[item_id];

    Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

    _state = BenchmarkState{_config.max_duration};

    while (_state.keep_running() &&
           result.num_iterations.load(std::memory_order_relaxed) < _config.max_num_query_runs) {
      // We want to only schedule as many queries simultaneously as we have simulated clients
      if (_currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
        _schedule_item_run(item_id);
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    _state.set_done();
    result.duration_ns.store(std::chrono::duration_cast<std::chrono::nanoseconds>(_state.benchmark_duration).count());

    const auto duration_seconds = static_cast<float>(result.duration_ns) / 1'000'000'000;
    const auto items_per_second = static_cast<float>(result.num_iterations) / duration_seconds;

    std::cout << "  -> Executed " << result.num_iterations << " times in " << duration_seconds << " seconds ("
              << items_per_second << " iter/s)" << std::endl;

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
        const auto item_run_begin = std::chrono::steady_clock::now();

        auto [metrics, any_verification_failed] = _benchmark_item_runner->execute_item(item_id);

        --_currently_running_clients;
        ++_total_finished_runs;

        result.verification_passed = !any_verification_failed;

        if (!_state.is_done()) {  // To prevent queries to add their result after the time is up
          result.num_iterations++;

          const auto duration = std::chrono::steady_clock::now() - item_run_begin;
          result.duration_ns.fetch_add(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());

          result.metrics.push_back(std::move(metrics));
        }
      },
      SchedulePriority::High);

  // No need to check if the benchmark uses the scheduler or not as this method executes tasks immediately if the
  // scheduler is not set.
  CurrentScheduler::schedule_tasks<JobTask>({task});
}

void BenchmarkRunner::_warmup_query(const BenchmarkItemID item_id) {
  if (_config.warmup_duration == Duration{0}) return;

  const auto& name = _benchmark_item_runner->item_name(item_id);
  BenchmarkItemResult& result = _results[item_id];
  std::cout << "- Warming up for Query " << name << std::endl;

  Assert(_currently_running_clients == 0, "Did not expect any clients to run at this time");

  _state = BenchmarkState{_config.warmup_duration};

  while (_state.keep_running() &&
         result.num_iterations.load(std::memory_order_relaxed) < _config.max_num_query_runs) {
    // We want to only schedule as many queries simultaneously as we have simulated clients
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
  Assert(_currently_running_clients == 0, "All query runs must be finished at this point");
}

void BenchmarkRunner::_create_report(std::ostream& stream) const {
  nlohmann::json benchmarks;

  for (const auto& item_id : _benchmark_item_runner->selected_items()) {
    const auto& name = _benchmark_item_runner->item_name(item_id);
    const auto& result = _results[item_id];
    Assert(result.metrics.size() == result.num_iterations,
           "number of iterations and number of iteration durations does not match");

    const auto duration_seconds = static_cast<float>(result.duration_ns) / 1'000'000'000;
    const auto items_per_second = static_cast<float>(result.num_iterations) / duration_seconds;
    const auto time_per_query =
        result.num_iterations > 0 ? static_cast<float>(result.duration_ns) / result.num_iterations : std::nanf("");

    // Convert the SQLPipelineMetrics for each run of the BenchmarkItem into JSON
    auto all_runs_json = nlohmann::json::array();

    for (const auto& item_metrics : result.metrics) {
      auto all_pipeline_metrics_json = nlohmann::json::array();

      for (const auto& run_metrics : item_metrics) {
        // clang-format off
        auto run_metrics_json = nlohmann::json{
          {"parse_duration", run_metrics.parse_time_nanos.count()},
          {"statements", nlohmann::json::array()}
        };

        for (const auto& statement_metrics : run_metrics.statement_metrics) {
          auto statement_metrics_json = nlohmann::json{
            {"sql_translation_duration", statement_metrics->sql_translation_duration.count()},
            {"optimization_duration", statement_metrics->optimization_duration.count()},
            {"lqp_translation_duration", statement_metrics->lqp_translation_duration.count()},
            {"plan_execution_duration", statement_metrics->plan_execution_duration.count()},
            {"query_plan_cache_hit", statement_metrics->query_plan_cache_hit}
          };

          run_metrics_json["statements"].push_back(statement_metrics_json);
        }
        // clang-format on

        all_pipeline_metrics_json.push_back(run_metrics_json);
      }
      all_runs_json.push_back(all_pipeline_metrics_json);
    }

    nlohmann::json benchmark{{"name", name},
                             {"iterations", result.num_iterations.load()},
                             {"metrics", all_runs_json},
                             {"avg_real_time_per_iteration", time_per_query},
                             {"items_per_second", items_per_second}};

    if (_config.verify) {
      Assert(result.verification_passed, "Verification should have been performed");
      benchmark["verification_passed"] = *result.verification_passed;
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
      {"total_run_duration", std::chrono::duration_cast<std::chrono::nanoseconds>(_total_run_duration).count()}};

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
    ("r,runs", "Maximum number of runs - per query for IndividualQueries, total for PermutedQuerySet", cxxopts::value<size_t>()->default_value("10000")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 100,000", cxxopts::value<ChunkOffset>()->default_value(std::to_string(Chunk::DEFAULT_SIZE))) // NOLINT
    ("t,time", "Maximum seconds that a query (set) is run", cxxopts::value<size_t>()->default_value("60")) // NOLINT
    ("w,warmup", "Number of seconds that each query is run for warm up", cxxopts::value<size_t>()->default_value("0")) // NOLINT
    ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("m,mode", "IndividualQueries or PermutedQuerySet, default is IndividualQueries", cxxopts::value<std::string>()->default_value("IndividualQueries")) // NOLINT
    ("e,encoding", "Specify Chunk encoding as a string or as a JSON config file (for more detailed configuration, see --full_help). String options: " + encoding_strings_option, cxxopts::value<std::string>()->default_value("Dictionary"))  // NOLINT
    ("compression", "Specify vector compression as a string. Options: " + compression_strings_option, cxxopts::value<std::string>()->default_value(""))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cores", "Specify the number of cores used by the scheduler (if active). 0 means all available cores", cxxopts::value<uint>()->default_value("0")) // NOLINT
    ("clients", "Specify how many queries should run in parallel if the scheduler is active", cxxopts::value<uint>()->default_value("1")) // NOLINT
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
      {"benchmark_mode",
       config.benchmark_mode == BenchmarkMode::IndividualQueries ? "IndividualQueries" : "PermutedQuerySet"},
      {"max_runs", config.max_num_query_runs},
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
