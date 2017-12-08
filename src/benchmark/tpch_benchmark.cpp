#include <chrono>
#include <fstream>
#include <iostream>
#include <random>
#include <string>

#include "cxxopts.hpp"
#include "json.hpp"

#include <boost/program_options.hpp>
#include <SQLParserResult.h>
#include <SQLParser.h>

#include "sql/sql_pipeline.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

namespace opossum {

enum class BenchmarkMode {
  IndividualQueries,
  PermutedQuerySets
};

using Duration = std::chrono::high_resolution_clock::duration;
using TimePoint = std::chrono::high_resolution_clock::time_point;

struct QueryBenchmarkResult {
  explicit QueryBenchmarkResult(size_t id): id(id) {}

  size_t id;
  size_t num_iterations = 0;
  Duration duration = Duration{};
};

using BenchmarkResults = std::unordered_map<size_t, QueryBenchmarkResult>;

// Loosely copying benchmark::State
struct BenchmarkState {
  enum class State {
    NotStarted, Running, Over
  };

  BenchmarkState(size_t max_num_iterations, Duration max_duration):
    max_num_iterations(max_num_iterations),
    max_duration(max_duration) {

  }

  bool keep_running() {
    switch (state) {
      case State::NotStarted:
        begin = std::chrono::high_resolution_clock::now();
        state = State::Running;
        break;
      case State::Over:
        return false;
      default:;
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
  size_t max_num_iterations = 0;
  Duration max_duration;
};

class TpchBenchmark {
 public:
  TpchBenchmark(BenchmarkMode benchmark_mode, const std::optional<std::string>& output_file_path, const std::vector<size_t>& query_ids):
    _benchmark_mode(benchmark_mode), _output_file_path(output_file_path), _query_ids(query_ids) {

  }

  void run(const opossum::ChunkOffset chunk_size,
           const float scale_factor,
           const size_t max_num_query_runs,
           const Duration max_duration
           ) {
    /**
     * Populate the StorageManager
     */
    std::cout << "- Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;
    opossum::TpchDbGenerator(scale_factor, chunk_size).generate_and_store();
    std::cout << "- Done." << std::endl;

    /**
     * Run the TPCH queries in the selected mode
     */
    if (_benchmark_mode == BenchmarkMode::IndividualQueries) {
      for (const auto query_id : _query_ids) {
        std::cout << "- Benchmarking Query " << (query_id + 1) << std::endl;

        const auto sql = opossum::tpch_queries[query_id];

        BenchmarkState state{max_num_query_runs, max_duration};
        while (state.keep_running()) {
          // Execute the query, we don't care about the results
          SQLPipeline{sql}.get_result_table();
        }

        QueryBenchmarkResult result{query_id};
        result.num_iterations = state.num_iterations;
        result.duration = std::chrono::high_resolution_clock::now() - state.begin;

        _query_results_by_query_id.emplace(query_id, result);
      }
    } else if (_benchmark_mode == BenchmarkMode::PermutedQuerySets) {
      // Init results
      for (const auto query_id : _query_ids) {
        _query_results_by_query_id.emplace(query_id, query_id);
      }

      auto mutable_query_ids = _query_ids;

      std::random_device random_device;
      std::mt19937 random_generator(random_device());

      BenchmarkState state{max_num_query_runs, max_duration};
      while (state.keep_running()) {
        std::shuffle(mutable_query_ids.begin(), mutable_query_ids.end(), random_generator);

        for (const auto query_id : mutable_query_ids) {
          const auto query_benchmark_begin = std::chrono::steady_clock::now();
          // Execute the query, we don't care about the results
          SQLPipeline{opossum::tpch_queries[query_id]}.get_result_table();
          const auto query_benchmark_end = std::chrono::steady_clock::now();

          auto& query_benchmark_result = _query_results_by_query_id.at(query_id);
          query_benchmark_result.duration += query_benchmark_end - query_benchmark_begin;
          query_benchmark_result.num_iterations++;
        }
      }
    } else {
      Fail("Invalid mode");
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
  BenchmarkMode _benchmark_mode;
  std::optional<std::string> _output_file_path;
  std::vector<size_t> _query_ids;
  BenchmarkResults _query_results_by_query_id;

  void _create_report(std::ostream & stream) const {
    nlohmann::json benchmarks;

    for (const auto query_id : _query_ids) {
      const auto& query_result = _query_results_by_query_id.at(query_id);

      const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(query_result.duration).count();
      const auto duration_seconds = static_cast<float>(duration_ns) / 1'000'000'000;
      const auto items_per_second = static_cast<float>(query_result.num_iterations) / duration_seconds;

      nlohmann::json benchmark{
      {"name", "TPC-H " + std::to_string(query_id + 1)},
      {"iterations", query_result.num_iterations},
      {"real_time", duration_ns / query_result.num_iterations},
      {"items_per_second", items_per_second},
      };

      benchmarks.push_back(benchmark);
    }

    nlohmann::json report{
    {"benchmarks", benchmarks}
    };

    stream << std::setw(4) << report << std::endl;
  }
};

}  // namespace

int main(int argc, char * argv[]) {
  auto num_runs = size_t{0};
  auto time_seconds = size_t{0};
  auto scale_factor = 1.0f;
  auto chunk_size = opossum::ChunkOffset(opossum::INVALID_CHUNK_OFFSET);
  auto benchmark_mode_str = std::string{"IndividualQueries"};

  cxxopts::Options cli_options_description{"TPCH Benchmark", ""};

  cli_options_description.add_options()
    ("help", "print this help message")
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>(scale_factor)->default_value("0.001"))
    ("r,runs", "Minimum number of runs of a single query", cxxopts::value<size_t>(num_runs)->default_value("150"))
    ("c,chunk_size", "ChunkSize, default is 2^32-1", cxxopts::value<opossum::ChunkOffset>(chunk_size)->default_value(std::to_string(opossum::INVALID_CHUNK_OFFSET)))
    ("t,time", "Minimum time spend exercising a query", cxxopts::value<size_t>(time_seconds)->default_value("5"))
    ("o,output", "File to output results to, default is stdout", cxxopts::value<std::string>())
    ("m,mode", "IndividualQueries or PermutedQuerySets, default is IndividualQueries", cxxopts::value<std::string>(benchmark_mode_str)->default_value(benchmark_mode_str))
    ("queries", "Specify queries to run, default is all that are supported", cxxopts::value<std::vector<size_t>>())
    ;

  cli_options_description.parse_positional("queries");
  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({}) << std::endl;
    return 0;
  }

  std::optional<std::string> output_file_path;
  if (cli_parse_result.count("output") > 0) {
    output_file_path = cli_parse_result["output"].as<std::string>();
    std::cout << "- Writing benchmark results to '" << *output_file_path << "'" << std::endl;
  } else {
    std::cout << "- Writing benchmark results stdout" << std::endl;
  }

  std::vector<size_t> query_ids;
  if (cli_parse_result.count("queries")) {
    const auto cli_query_ids = cli_parse_result["queries"].as<std::vector<size_t>>();
    for (const auto cli_query_id : cli_query_ids) {
      query_ids.emplace_back(cli_query_id - 1); // Offset because TPC-H query 1 has index 0
    }
  } else {
    std::copy(std::begin(opossum::tpch_supported_queries), std::end(opossum::tpch_supported_queries), std::back_inserter(query_ids));
  }
  std::cout << "- Benchmarking Queries ";
  for (const auto query_id : query_ids) {
    std::cout << (query_id + 1) << " ";
  }
  std::cout << std::endl;

  auto benchmark_mode = opossum::BenchmarkMode::IndividualQueries; // Just init deterministically
  if (benchmark_mode_str == "IndividualQueries") {
    benchmark_mode = opossum::BenchmarkMode::IndividualQueries;
  } else if (benchmark_mode_str == "PermutedQuerySets") {
    benchmark_mode = opossum::BenchmarkMode::PermutedQuerySets;
  } else {
    std::cout << "ERROR: Invalid benchmark mode: '" << benchmark_mode_str << "'" << std::endl;
    std::cout << cli_options_description.help({}) << std::endl;
    return 1;
  }
  std::cout << "- Running benchmark in '" << benchmark_mode_str << "' mode" << std::endl;


  opossum::TpchBenchmark(benchmark_mode, output_file_path, query_ids).run(
    chunk_size,
    scale_factor,
    num_runs,
    std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{time_seconds}));

  return 0;
}