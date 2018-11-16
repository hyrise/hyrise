#include <json.hpp>

#include <random>

#include "benchmark_runner.hpp"
#include "constant_mappings.hpp"
#include "scheduler/current_scheduler.hpp"
#include "sql/create_sql_parser_error_message.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "version.hpp"
#include "visualization/lqp_visualizer.hpp"
#include "visualization/sql_query_plan_visualizer.hpp"

namespace opossum {

BenchmarkRunner::BenchmarkRunner(const BenchmarkConfig& config, std::unique_ptr<AbstractQueryGenerator> query_generator,
                                 const nlohmann::json& context)
    : _config(config), _query_generator(std::move(query_generator)), _context(context) {
  // In non-verbose mode, disable performance warnings
  if (!config.verbose) {
    _performance_warning_disabler.emplace();
  }

  // Initialise the scheduler if the benchmark was requested to run multi-threaded
  if (config.enable_scheduler) {
    Topology::use_default_topology(config.cores);
    config.out << "- Multi-threaded Topology:" << std::endl;
    Topology::get().print(config.out, 2);

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
  _config.out << "\n- Starting Benchmark..." << std::endl;

  const auto available_queries_count = _query_generator->available_query_count();
  _query_plans.resize(available_queries_count);
  _query_results.resize(available_queries_count);

  auto benchmark_start = std::chrono::steady_clock::now();

  // Run the queries in the selected mode
  switch (_config.benchmark_mode) {
    case BenchmarkMode::IndividualQueries: {
      _benchmark_individual_queries();
      break;
    }
    case BenchmarkMode::PermutedQuerySet: {
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
  } else {
    _create_report(std::cout);
  }

  // Visualize query plans
  if (_config.enable_visualization) {
    for (auto query_id = QueryID{0}; query_id < _query_plans.size(); ++query_id) {
      const auto& lqps = _query_plans[query_id].lqps;
      const auto& pqps = _query_plans[query_id].pqps;

      if (lqps.empty()) continue;

      auto name = _query_generator->query_names()[query_id];
      boost::replace_all(name, " ", "_");

      GraphvizConfig graphviz_config;
      graphviz_config.format = "svg";

      for (auto lqp_idx = size_t{0}; lqp_idx < lqps.size(); ++lqp_idx) {
        const auto file_prefix = name + "-LQP-" + std::to_string(lqp_idx);
        LQPVisualizer{graphviz_config, {}, {}, {}}.visualize({lqps[lqp_idx]}, file_prefix + ".dot",
                                                             file_prefix + ".svg");
      }
      for (auto pqp_idx = size_t{0}; pqp_idx < pqps.size(); ++pqp_idx) {
        const auto file_prefix = name + "-PQP-" + std::to_string(pqp_idx);
        SQLQueryPlanVisualizer{graphviz_config, {}, {}, {}}.visualize(*pqps[pqp_idx], file_prefix + ".dot",
                                                                      file_prefix + ".svg");
      }
    }
  }
}

void BenchmarkRunner::_benchmark_permuted_query_set() {
  const auto number_of_queries = _query_generator->selected_query_count();
  auto query_ids = _query_generator->selected_queries();

  for (const auto& query_id : query_ids) {
    _warmup_query(query_id);
  }

  // For shuffling the query order
  std::random_device random_device;
  std::mt19937 random_generator(random_device());

  // The atomic uints are modified by other threads when finishing a query set, to keep track of when we can
  // let a simulated client schedule the next set, as well as the total number of finished query sets so far
  auto currently_running_clients = std::atomic_uint{0};
  auto finished_query_set_runs = std::atomic_uint{0};
  auto finished_queries_total = std::atomic_uint{0};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto state = BenchmarkState{_config.max_duration};

  while (state.keep_running() && finished_query_set_runs.load(std::memory_order_relaxed) < _config.max_num_query_runs) {
    // We want to only schedule as many query sets simultaneously as we have simulated clients
    if (currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
      currently_running_clients++;
      std::shuffle(query_ids.begin(), query_ids.end(), random_generator);

      for (const auto& query_id : query_ids) {
        // The on_query_done callback will be appended to the last Task of the query,
        // to measure its duration as well as signal that the query was finished
        const auto query_run_begin = std::chrono::steady_clock::now();
        auto on_query_done = [query_run_begin, query_id, number_of_queries, &currently_running_clients,
                              &finished_query_set_runs, &finished_queries_total, &state, this]() {
          if (finished_queries_total++ % number_of_queries == 0) {
            currently_running_clients--;
            finished_query_set_runs++;
          }

          if (!state.is_done()) {  // To prevent queries to add their results after the time is up
            const auto duration = std::chrono::steady_clock::now() - query_run_begin;
            auto& result = _query_results[query_id];
            result.duration += duration;
            result.iteration_durations.push_back(duration);
            result.num_iterations++;
          }
        };

        auto query_tasks = _schedule_or_execute_query(query_id, on_query_done);
        tasks.insert(tasks.end(), query_tasks.begin(), query_tasks.end());
      }
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  state.set_done();

  // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
  // TODO(leander/anyone): To be replaced with something like CurrentScheduler::abort(),
  // that properly removes all remaining tasks from all queues, without having to wait for them
  CurrentScheduler::wait_for_tasks(tasks);
  Assert(currently_running_clients == 0, "All query set runs must be finished at this point");
}

void BenchmarkRunner::_benchmark_individual_queries() {
  for (const auto& query_id : _query_generator->selected_queries()) {
    _warmup_query(query_id);

    const auto& name = _query_generator->query_names()[query_id];
    _config.out << "- Benchmarking Query " << name << std::endl;

    // The atomic uints are modified by other threads when finishing a query, to keep track of when we can
    // let a simulated client schedule the next query, as well as the total number of finished queries so far
    auto currently_running_clients = std::atomic_uint{0};
    auto& result = _query_results[query_id];

    auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
    auto state = BenchmarkState{_config.max_duration};

    while (state.keep_running() && result.num_iterations.load(std::memory_order_relaxed) < _config.max_num_query_runs) {
      // We want to only schedule as many queries simultaneously as we have simulated clients
      if (currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
        currently_running_clients++;

        // The on_query_done callback will be appended to the last Task of the query,
        // to measure its duration as well as signal that the query was finished
        const auto query_run_begin = std::chrono::steady_clock::now();
        auto on_query_done = [query_run_begin, &currently_running_clients, &result, &state]() {
          currently_running_clients--;
          if (!state.is_done()) {  // To prevent queries to add their results after the time is up
            const auto query_run_end = std::chrono::steady_clock::now();
            result.num_iterations++;
            result.iteration_durations.push_back(query_run_end - query_run_begin);
          }
        };

        auto query_tasks = _schedule_or_execute_query(query_id, on_query_done);
        tasks.insert(tasks.end(), query_tasks.begin(), query_tasks.end());
      } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    state.set_done();
    result.duration = state.benchmark_duration;

    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(result.duration).count();
    const auto duration_seconds = static_cast<float>(duration_ns) / 1'000'000'000;
    const auto items_per_second = static_cast<float>(result.num_iterations) / duration_seconds;

    _config.out << "  -> Executed " << result.num_iterations << " times in " << duration_seconds << " seconds ("
                << items_per_second << " iter/s)" << std::endl;

    // Wait for the rest of the tasks that didn't make it in time - they will not count toward the results
    // TODO(leander/anyone): To be replaced with something like CurrentScheduler::abort(),
    // that properly removes all remaining tasks from all queues, without having to wait for them
    CurrentScheduler::wait_for_tasks(tasks);
    Assert(currently_running_clients == 0, "All query runs must be finished at this point");
  }
}

void BenchmarkRunner::_warmup_query(const QueryID query_id) {
  if (_config.warmup_duration == Duration{0}) {
    return;
  }

  const auto& name = _query_generator->query_names()[query_id];
  _config.out << "- Warming up for Query " << name << std::endl;

  // The atomic uints are modified by other threads when finishing a query, to keep track of when we can
  // let a simulated client schedule the next query, as well as the total number of finished queries so far
  auto currently_running_clients = std::atomic_uint{0};

  auto tasks = std::vector<std::shared_ptr<AbstractTask>>{};
  auto state = BenchmarkState{_config.warmup_duration};

  while (state.keep_running()) {
    // We want to only schedule as many queries simultaneously as we have simulated clients
    if (currently_running_clients.load(std::memory_order_relaxed) < _config.clients) {
      currently_running_clients++;

      // The on_query_done callback will be appended to the last Task of the query,
      // to signal that the query was finished
      auto on_query_done = [&currently_running_clients]() { currently_running_clients--; };

      auto query_tasks = _schedule_or_execute_query(query_id, on_query_done);
      tasks.insert(tasks.end(), query_tasks.begin(), query_tasks.end());
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  state.set_done();

  // Wait for the rest of the tasks that didn't make it in time
  // TODO(leander/anyone): To be replaced with something like CurrentScheduler::abort(),
  // that properly removes all remaining tasks from all queues, without having to wait for them
  CurrentScheduler::wait_for_tasks(tasks);
  Assert(currently_running_clients == 0, "All query runs must be finished at this point");
}

std::vector<std::shared_ptr<AbstractTask>> BenchmarkRunner::_schedule_or_execute_query(
    const QueryID query_id, const std::function<void()>& done_callback) {
  // Some queries (like TPC-H 15) require execution before we can call get_tasks() on the pipeline.
  // These queries can't be scheduled yet, therefore we fall back to "just" executing the query
  // when we don't use the scheduler anyway, so that they can be executed.
  if (_config.enable_scheduler) {
    return _schedule_query(query_id, done_callback);
  }
  _execute_query(query_id, done_callback);
  return {};
}

std::vector<std::shared_ptr<AbstractTask>> BenchmarkRunner::_schedule_query(
    const QueryID query_id, const std::function<void()>& done_callback) {
  const auto& sql = _query_generator->build_query(query_id);

  auto query_tasks = std::vector<std::shared_ptr<AbstractTask>>();

  auto pipeline_builder = SQLPipelineBuilder{sql}.with_mvcc(_config.use_mvcc);
  if (_config.enable_visualization) pipeline_builder.dont_cleanup_temporaries();
  auto pipeline = pipeline_builder.create_pipeline();

  auto tasks_per_statement = pipeline.get_tasks();
  tasks_per_statement.back().back()->set_done_callback(done_callback);

  for (auto tasks : tasks_per_statement) {
    CurrentScheduler::schedule_tasks(tasks);
    query_tasks.insert(query_tasks.end(), tasks.begin(), tasks.end());
  }

  // If necessary, keep plans for visualization
  _store_plan(query_id, pipeline);

  return query_tasks;
}

void BenchmarkRunner::_execute_query(const QueryID query_id, const std::function<void()>& done_callback) {
  const auto& sql = _query_generator->build_query(query_id);

  auto pipeline_builder = SQLPipelineBuilder{sql}.with_mvcc(_config.use_mvcc);
  if (_config.enable_visualization) pipeline_builder.dont_cleanup_temporaries();
  auto pipeline = pipeline_builder.create_pipeline();
  // Execute the query, we don't care about the results
  pipeline.get_result_table();

  if (done_callback) done_callback();

  // If necessary, keep plans for visualization
  _store_plan(query_id, pipeline);
}

void BenchmarkRunner::_store_plan(const QueryID query_id, SQLPipeline& pipeline) {
  if (_config.enable_visualization) {
    if (_query_plans[query_id].lqps.empty()) {
      QueryPlans plans{pipeline.get_optimized_logical_plans(), pipeline.get_query_plans()};
      _query_plans[query_id] = plans;
    }
  }
}

void BenchmarkRunner::_create_report(std::ostream& stream) const {
  nlohmann::json benchmarks;

  for (const auto& query_id : _query_generator->selected_queries()) {
    const auto& name = _query_generator->query_names()[query_id];
    const auto& query_result = _query_results[query_id];
    Assert(query_result.iteration_durations.size() == query_result.num_iterations,
           "number of iterations and number of iteration durations does not match");

    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(query_result.duration).count();
    const auto duration_seconds = static_cast<float>(duration_ns) / 1'000'000'000;
    const auto items_per_second = static_cast<float>(query_result.num_iterations) / duration_seconds;
    const auto time_per_query =
        query_result.num_iterations > 0 ? static_cast<float>(duration_ns) / query_result.num_iterations : std::nanf("");

    // Transform iteration Durations into numerical representation
    auto iteration_durations = std::vector<double>();
    iteration_durations.reserve(query_result.iteration_durations.size());
    std::transform(query_result.iteration_durations.cbegin(), query_result.iteration_durations.cend(),
                   std::back_inserter(iteration_durations), [](const auto& duration) {
                     return static_cast<double>(std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count());
                   });

    nlohmann::json benchmark{{"name", name},
                             {"iterations", query_result.num_iterations.load()},
                             {"iteration_durations", iteration_durations},
                             {"avg_real_time_per_iteration", time_per_query},
                             {"items_per_second", items_per_second},
                             {"time_unit", "ns"}};

    benchmarks.push_back(benchmark);
  }

  // Gather information on the (estimated) table size
  auto table_size = 0ull;
  for (const auto& table_pair : StorageManager::get().tables()) {
    table_size += table_pair.second->estimate_memory_usage();
  }

  const auto total_run_duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(_total_run_duration).count();

  nlohmann::json summary{{"table_size_in_bytes", table_size}, {"total_run_duration_in_s", total_run_duration_seconds}};

  nlohmann::json report{{"context", _context}, {"benchmarks", benchmarks}, {"summary", summary}};

  stream << std::setw(2) << report << std::endl;
}

cxxopts::Options BenchmarkRunner::get_basic_cli_options(const std::string& benchmark_name) {
  cxxopts::Options cli_options{benchmark_name};

  // Make sure all current encoding types are shown
  std::vector<std::string> encoding_strings;
  encoding_strings.reserve(encoding_type_to_string.right.size());
  for (const auto& encoding : encoding_type_to_string.right) {
    encoding_strings.emplace_back(encoding.first);
  }

  const auto encoding_strings_option = boost::algorithm::join(encoding_strings, ", ");

  // Make sure all current compression types are shown
  std::vector<std::string> compression_strings;
  compression_strings.reserve(vector_compression_type_to_string.right.size());
  for (const auto& vector_compression : vector_compression_type_to_string.right) {
    compression_strings.emplace_back(vector_compression.first);
  }

  const auto compression_strings_option = boost::algorithm::join(compression_strings, ", ");

  // If you add a new option here, make sure to edit CLIConfigParser::basic_cli_options_to_json() so it contains the
  // newest options. Sadly, there is no way to to get all option keys to do this automatically.
  // clang-format off
  cli_options.add_options()
    ("help", "print this help message")
    ("v,verbose", "Print log messages", cxxopts::value<bool>()->default_value("false"))
    ("r,runs", "Maximum number of runs of a single query (set)", cxxopts::value<size_t>()->default_value("10000")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 100,000", cxxopts::value<ChunkOffset>()->default_value("100000")) // NOLINT
    ("t,time", "Maximum seconds that a query (set) is run", cxxopts::value<size_t>()->default_value("60")) // NOLINT
    ("w,warmup", "Number of seconds that each query is run for warm up", cxxopts::value<size_t>()->default_value("0")) // NOLINT
    ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>()->default_value("")) // NOLINT
    ("m,mode", "IndividualQueries or PermutedQuerySet, default is IndividualQueries", cxxopts::value<std::string>()->default_value("IndividualQueries")) // NOLINT
    ("e,encoding", "Specify Chunk encoding as a string or as a JSON config file (for more detailed configuration, see below). String options: " + encoding_strings_option, cxxopts::value<std::string>()->default_value("Dictionary"))  // NOLINT
    ("compression", "Specify vector compression as a string. Options: " + compression_strings_option, cxxopts::value<std::string>()->default_value(""))  // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("cores", "Specify the number of cores used by the scheduler (if active). 0 means all available cores", cxxopts::value<uint>()->default_value("0")) // NOLINT
    ("clients", "Specify how many queries should run in parallel if the scheduler is active", cxxopts::value<uint>()->default_value("1")) // NOLINT
    ("mvcc", "Enable MVCC", cxxopts::value<bool>()->default_value("false")) // NOLINT
    ("visualize", "Create a visualization image of one LQP and PQP for each query", cxxopts::value<bool>()->default_value("false")); // NOLINT
  // clang-format on

  return cli_options;
}

nlohmann::json BenchmarkRunner::create_context(const BenchmarkConfig& config) {
  // Generate YY-MM-DD hh:mm::ss
  auto current_time = std::time(nullptr);
  auto local_time = *std::localtime(&current_time);
  std::stringstream timestamp_stream;
  timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

  std::stringstream compiler;
  // clang-format off
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
      {"build_type", IS_DEBUG ? "debug" : "release"},
      {"encoding", config.encoding_config.to_json()},
      {"benchmark_mode",
       config.benchmark_mode == BenchmarkMode::IndividualQueries ? "IndividualQueries" : "PermutedQuerySet"},
      {"max_runs", config.max_num_query_runs},
      {"max_duration_in_s", std::chrono::duration_cast<std::chrono::seconds>(config.max_duration).count()},
      {"warmup_duration_in_s", std::chrono::duration_cast<std::chrono::seconds>(config.warmup_duration).count()},
      {"using_mvcc", config.use_mvcc == UseMvcc::Yes},
      {"using_visualization", config.enable_visualization},
      {"output_file_path", config.output_file_path ? *(config.output_file_path) : "stdout"},
      {"using_scheduler", config.enable_scheduler},
      {"cores", config.cores},
      {"clients", config.clients},
      {"verbose", config.verbose},
      {"GIT-HASH", GIT_HEAD_SHA1 + std::string(GIT_IS_DIRTY ? "-dirty" : "")}};
}

}  // namespace opossum
