#include <chrono>
#include <iostream>
#include <random>

#include "cxxopts.hpp"

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

void create_report(const BenchmarkResults& results) {

}

void tpch_benchmark(BenchmarkMode benchmark_mode,
                    const opossum::ChunkOffset chunk_size,
                    const float scale_factor,
                    const size_t max_num_query_runs,
                    const Duration max_duration,
                    const std::vector<size_t>& query_ids) {
  /**
   * Populate the StorageManager
   */
  std::cout << "Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;
  opossum::TpchDbGenerator(scale_factor, chunk_size).generate_and_store();
  std::cout << "Done." << std::endl;

  BenchmarkResults query_results_by_query_id;

  if (benchmark_mode == BenchmarkMode::IndividualQueries) {
    for (const auto query_id : query_ids) {
      const auto sql = opossum::tpch_queries[query_id];

      BenchmarkState state{max_num_query_runs, max_duration};
      while (state.keep_running()) {
        // Execute the query, we don't care about the results
        SQLPipeline{sql}.get_result_table();
      }

      QueryBenchmarkResult result{query_id};
      result.num_iterations = state.num_iterations;
      result.duration = std::chrono::high_resolution_clock::now() - state.begin;

      query_results_by_query_id.emplace(query_id, result);
    }
  } else if (benchmark_mode == BenchmarkMode::PermutedQuerySets) {
    // Init results
    for (const auto query_id : query_ids) {
      query_results_by_query_id.emplace(query_id, query_id);
    }

    auto mutable_query_ids = query_ids;

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

          auto& query_benchmark_result = query_results_by_query_id.at(query_id);
          query_benchmark_result.duration += query_benchmark_end - query_benchmark_begin;
          query_benchmark_result.num_iterations++;
        }
      }
  } else {
    Fail("Invalid mode");
  }

  create_report(query_results_by_query_id);
}

}  // namespace

int main(int argc, char * argv[]) {
  auto num_runs = size_t{0};
  auto time_seconds = size_t{0};
  auto scale_factor = 1.0f;
  auto chunk_size = opossum::ChunkOffset(opossum::INVALID_CHUNK_OFFSET);

  cxxopts::Options cli_options_description{"TPCH Benchmark", ""};

  cli_options_description.add_options()
    ("help", "print this help message")
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>(scale_factor)->default_value("0.001"))
    ("r,runs", "Minimum number of runs of a single query", cxxopts::value<size_t>(num_runs)->default_value("2"))
    ("c,chunk_size", "ChunkSize, defaults is 2^32-1", cxxopts::value<opossum::ChunkOffset>(chunk_size)->default_value(std::to_string(opossum::INVALID_CHUNK_OFFSET)))
    ("t,time", "Minimum time spend exercising a query", cxxopts::value<size_t>(time_seconds)->default_value("60"))
    ;

  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({"", "Group"}) << std::endl;
    return 0;
  }

  opossum::tpch_benchmark(
    opossum::BenchmarkMode::IndividualQueries,
    chunk_size,
    scale_factor,
    num_runs,
    std::chrono::duration_cast<opossum::Duration>(std::chrono::seconds{time_seconds}), {6,7});

  return 0;
}