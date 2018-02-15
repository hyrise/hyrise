#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "cxxopts.hpp"
#include "json.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql_pipeline.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

/**
 * This benchmark measures Hyrise's performance executing the TPC-H queries, it doesn't (yet) support running the TPC-H
 * benchmark as it is specified.
 * (Among other things, the TPC-H requires performing data refreshes and has strict requirements for the number of
 * sessions running in parallel. See http://www.tpc.org/tpch/default.asp for more info)
 * The benchmark offers a wide range of options (scale_factor, chunk_size, ...) but most notably it offers two modes:
 * IndividualQueries and PermutedQuerySets. See docs on BenchmarkMode for details.
 * The benchmark will stop issuing new queries if either enough iterations have taken place or enough time has passed.
 *
 * main() is mostly concerned with parsing the CLI options while TpchBenchmark::run() performs the actual benchmark
 * logic.
 */

namespace opossum {

/**
 * IndividualQueries runs each query a number of times and then the next one
 * PermutedQuerySets runs the queries as sets permuting their order after each run (this exercises caches)
 */
enum class BenchmarkMode { IndividualQueries, PermutedQuerySets };

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

// Used to config out()
auto verbose_benchmark = false;

/**
 * @return std::cout if `verbose_benchmark` is true, otherwise returns a discarding stream
 */
std::ostream& out() {
  if (verbose_benchmark) {
    return std::cout;
  }

  // Create no-op stream that just swallows everything streamed into it
  // See https://stackoverflow.com/a/11826666
  class NullBuffer : public std::streambuf {
   public:
    int overflow(int c) { return c; }
  };

  static NullBuffer null_buffer;
  static std::ostream null_stream(&null_buffer);

  return null_stream;
}

struct QueryBenchmarkResult {
  size_t num_iterations = 0;
  Duration duration = Duration{};
};

using QueryID = size_t;
using BenchmarkResults = std::unordered_map<QueryID, QueryBenchmarkResult>;

/**
 * Loosely copying the functionality of benchmark::State
 * keep_running() returns false once enough iterations or time has passed.
 *
 */
struct BenchmarkState {
  enum class State { NotStarted, Running, Over };

  BenchmarkState(const size_t max_num_iterations, const Duration max_duration)
      : max_num_iterations(max_num_iterations), max_duration(max_duration) {}

  bool keep_running() {
    switch (state) {
      case State::NotStarted:
        begin = std::chrono::high_resolution_clock::now();
        state = State::Running;
        break;
      case State::Over:
        return false;
      default: {}
    }

    if (num_iterations >= max_num_iterations) {
      end = std::chrono::high_resolution_clock::now();
      state = State::Over;
      return false;
    }

    end = std::chrono::high_resolution_clock::now();
    const auto duration = end - begin;
    if (duration >= max_duration) {
      state = State::Over;
      return false;
    }

    num_iterations++;

    return true;
  }

  State state{State::NotStarted};
  TimePoint begin = TimePoint{};
  TimePoint end = TimePoint{};

  size_t num_iterations = 0;
  size_t max_num_iterations;
  Duration max_duration;
};

// Stores config and state of a benchmark mode instantiation
class TpchBenchmark final {
 public:
  TpchBenchmark(const BenchmarkMode benchmark_mode, std::vector<QueryID> query_ids,
                const opossum::ChunkOffset chunk_size, const float scale_factor, const size_t max_num_query_runs,
                const Duration max_duration, const std::optional<std::string>& output_file_path, const UseMvcc use_mvcc)
      : _benchmark_mode(benchmark_mode),
        _query_ids(std::move(query_ids)),
        _chunk_size(chunk_size),
        _scale_factor(scale_factor),
        _max_num_query_runs(max_num_query_runs),
        _max_duration(max_duration),
        _output_file_path(output_file_path),
        _use_mvcc(use_mvcc),
        _query_results_by_query_id() {}

  void run() {
    /**
     * Populate the StorageManager with the TPC-H tables
     */
    out() << "- Generating TPCH Tables with scale_factor=" << _scale_factor << "..." << std::endl;
    opossum::TpchDbGenerator(_scale_factor, _chunk_size).generate_and_store();
    out() << "- Done." << std::endl;

    /**
     * Run the TPCH queries in the selected mode
     */
    switch (_benchmark_mode) {
      case BenchmarkMode::IndividualQueries:
        _benchmark_individual_queries();
        break;
      case BenchmarkMode::PermutedQuerySets:
        _benchmark_permuted_query_sets();
        break;
    }

    /**
     * Create report
     */
    if (_output_file_path) {
      std::ofstream output_file(*_output_file_path);
      _create_report(output_file);
    } else {
      _create_report(std::cout);
    }
  }

 private:
  const BenchmarkMode _benchmark_mode;
  const std::vector<QueryID> _query_ids;
  const ChunkOffset _chunk_size;
  const float _scale_factor;
  const size_t _max_num_query_runs;
  const Duration _max_duration;
  const std::optional<std::string> _output_file_path;
  const UseMvcc _use_mvcc;

  BenchmarkResults _query_results_by_query_id;

  // Run benchmark in BenchmarkMode::PermutedQuerySets mode
  void _benchmark_permuted_query_sets() {
    // Init results
    for (const auto query_id : _query_ids) {
      _query_results_by_query_id.emplace(query_id, QueryBenchmarkResult{});
    }

    auto mutable_query_ids = _query_ids;

    // For shuffling the query order
    std::random_device random_device;
    std::mt19937 random_generator(random_device());

    BenchmarkState state{_max_num_query_runs, _max_duration};
    while (state.keep_running()) {
      std::shuffle(mutable_query_ids.begin(), mutable_query_ids.end(), random_generator);

      for (const auto query_id : mutable_query_ids) {
        const auto query_benchmark_begin = std::chrono::steady_clock::now();

        // Execute the query, we don't care about the results
        SQLPipeline{opossum::tpch_queries[query_id], _use_mvcc}.get_result_table();

        const auto query_benchmark_end = std::chrono::steady_clock::now();

        auto& query_benchmark_result = _query_results_by_query_id.at(query_id);
        query_benchmark_result.duration += query_benchmark_end - query_benchmark_begin;
        query_benchmark_result.num_iterations++;
      }
    }
  }

  // Run benchmark in BenchmarkMode::IndividualQueries mode
  void _benchmark_individual_queries() {
    for (const auto query_id : _query_ids) {
      out() << "- Benchmarking Query " << (query_id + 1) << std::endl;

      const auto sql = opossum::tpch_queries[query_id];

      BenchmarkState state{_max_num_query_runs, _max_duration};
      while (state.keep_running()) {
        // Execute the query, we don't care about the results
        SQLPipeline{sql, _use_mvcc}.get_result_table();
      }

      QueryBenchmarkResult result;
      result.num_iterations = state.num_iterations;
      result.duration = std::chrono::high_resolution_clock::now() - state.begin;

      _query_results_by_query_id.emplace(query_id, result);
    }
  }

  // Create a report in roughly the same format as google benchmarks do when run with --benchmark_format=json
  void _create_report(std::ostream& stream) const {
    nlohmann::json benchmarks;

    for (const auto query_id : _query_ids) {
      const auto& query_result = _query_results_by_query_id.at(query_id);

      const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(query_result.duration).count();
      const auto duration_seconds = static_cast<float>(duration_ns) / 1'000'000'000;
      const auto items_per_second = static_cast<float>(query_result.num_iterations) / duration_seconds;
      const auto time_per_query = duration_ns / query_result.num_iterations;

      nlohmann::json benchmark{
          {"name", "TPC-H " + std::to_string(query_id + 1)},
          {"iterations", query_result.num_iterations},
          {"real_time", time_per_query},
          {"cpu_time", time_per_query},
          {"items_per_second", items_per_second},
          {"time_unit", "ns"},
      };

      benchmarks.push_back(benchmark);
    }

    /**
     * Generate YY-MM-DD hh:mm::ss
     */
    auto current_time = std::time(nullptr);
    auto local_time = *std::localtime(&current_time);
    std::stringstream timestamp_stream;
    timestamp_stream << std::put_time(&local_time, "%Y-%m-%d %H:%M:%S");

    nlohmann::json context{
        {"date", timestamp_stream.str()},
        {"scale_factor", _scale_factor},
        {"chunk_size", _chunk_size},
        {"build_type", IS_DEBUG ? "debug" : "release"},
        {"benchmark_mode",
         _benchmark_mode == BenchmarkMode::IndividualQueries ? "IndividualQueries" : "PermutedQuerySets"}};

    nlohmann::json report{{"context", context}, {"benchmarks", benchmarks}};

    stream << std::setw(2) << report << std::endl;
  }
};

}  // namespace opossum

int main(int argc, char* argv[]) {
  auto num_iterations = size_t{0};
  auto timeout_duration = size_t{0};
  auto scale_factor = 1.0f;
  auto chunk_size = opossum::ChunkOffset(opossum::INVALID_CHUNK_OFFSET);
  auto benchmark_mode_str = std::string{"IndividualQueries"};
  auto enable_mvcc = false;
  auto enable_scheduler = false;

  cxxopts::Options cli_options_description{"TPCH Benchmark", ""};

  // clang-format off
  cli_options_description.add_options()
    ("help", "print this help message")
    ("v,verbose", "Print log messages", cxxopts::value<bool>(opossum::verbose_benchmark)->default_value("false"))
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>(scale_factor)->default_value("0.001"))
    ("r,runs", "Maximum number of runs of a single query", cxxopts::value<size_t>(num_iterations)->default_value("1000")) // NOLINT
    ("c,chunk_size", "ChunkSize, default is 2^32-1", cxxopts::value<opossum::ChunkOffset>(chunk_size)->default_value(std::to_string(opossum::INVALID_CHUNK_OFFSET))) // NOLINT
    ("t,time", "Maximum seconds within which a new query(set) is initiated", cxxopts::value<size_t>(timeout_duration)->default_value("5")) // NOLINT
    ("o,output", "File to output results to, don't specify for stdout", cxxopts::value<std::string>())
    ("m,mode", "IndividualQueries or PermutedQuerySets, default is IndividualQueries", cxxopts::value<std::string>(benchmark_mode_str)->default_value(benchmark_mode_str)) // NOLINT
    ("mvcc", "Enable or disable MVCC", cxxopts::value<bool>(enable_mvcc)->default_value("false")) // NOLINT
    ("scheduler", "Enable or disable the scheduler", cxxopts::value<bool>(enable_scheduler)->default_value("false")) // NOLINT
    ("queries", "Specify queries to run, default is all that are supported", cxxopts::value<std::vector<opossum::QueryID>>()); // NOLINT
  // clang-format on

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  // Display usage and quit
  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  // In non-verbose mode, disable performance warnings
  std::optional<opossum::PerformanceWarningDisabler> performance_warning_disabler;
  if (!opossum::verbose_benchmark) {
    performance_warning_disabler.emplace();
  }

  // Display info about output destination
  std::optional<std::string> output_file_path;
  if (cli_parse_result.count("output") > 0) {
    output_file_path = cli_parse_result["output"].as<std::string>();
    opossum::out() << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  } else {
    opossum::out() << "- Writing benchmark results to stdout" << std::endl;
  }

  // Display info about MVCC being enabled or not
  opossum::out() << "- MVCC is " << (enable_mvcc ? "enabled" : "disabled") << std::endl;

  /**
   * Initialise the Scheduler if the Benchmark was requested to run multithreaded
   */
  if (enable_scheduler) {
    const auto topology = opossum::Topology::create_numa_topology();
    opossum::out() << "- Running in multi-threaded mode, with the following Topology:" << std::endl;
    topology->print(opossum::out());

    const auto scheduler = std::make_shared<opossum::NodeQueueScheduler>(topology);
    opossum::CurrentScheduler::set(scheduler);
  } else {
    opossum::out() << "- Running in single-threaded mode" << std::endl;
  }

  // Build list of query ids to be benchmarked and display it
  std::vector<opossum::QueryID> query_ids;
  if (cli_parse_result.count("queries")) {
    const auto cli_query_ids = cli_parse_result["queries"].as<std::vector<opossum::QueryID>>();
    for (const auto cli_query_id : cli_query_ids) {
      query_ids.emplace_back(cli_query_id - 1);  // Offset because TPC-H query 1 has index 0
    }
  } else {
    std::copy(std::begin(opossum::tpch_supported_queries), std::end(opossum::tpch_supported_queries),
              std::back_inserter(query_ids));
  }
  opossum::out() << "- Benchmarking Queries ";
  for (const auto query_id : query_ids) {
    opossum::out() << (query_id + 1) << " ";
  }
  opossum::out() << std::endl;

  // Determine benchmark and display it
  auto benchmark_mode = opossum::BenchmarkMode::IndividualQueries;  // Just to init it deterministically
  if (benchmark_mode_str == "IndividualQueries") {
    benchmark_mode = opossum::BenchmarkMode::IndividualQueries;
  } else if (benchmark_mode_str == "PermutedQuerySets") {
    benchmark_mode = opossum::BenchmarkMode::PermutedQuerySets;
  } else {
    std::cerr << "ERROR: Invalid benchmark mode: '" << benchmark_mode_str << "'" << std::endl;
    std::cerr << cli_options_description.help({}) << std::endl;
    return 1;
  }
  opossum::out() << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;

  // Run the benchmark
  opossum::TpchBenchmark(benchmark_mode, query_ids, chunk_size, scale_factor, num_iterations,
                         std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{timeout_duration}),
                         output_file_path, enable_mvcc ? opossum::UseMvcc::Yes : opossum::UseMvcc::No)
      .run();

  return 0;
}
