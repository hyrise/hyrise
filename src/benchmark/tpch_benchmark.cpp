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

class InidividualQueryBenchmark {
 public:
  void execute() {
    while (state.keep_running()) {
      // Execute the query, we don't care about the results
      SQLPipeline{sql}.get_result_table();
    }
    result.num_iterations = state.num_iterations;
    result.duration = state.duration;
  }
};

class PermutedQuerySetBenchmark {
 public:
  void execute() {
    std::random_device random_device;
    std::mt19937 random_generator(random_device());


    while (state.keep_running()) {
      std::shuffle(_query_ids.begin(), _query_ids.end(), random_generator);

      for (const auto query_id : _query_ids) {
        const auto query_benchmark_begin = std::chrono::steady_clock::now();
        // Execute the query, we don't care about the results
        SQLPipeline{opossum::tpch_queries[query_id]}.get_result_table();
        const auto query_benchmark_end = std::chrono::steady_clock::now();

        
        _results.at(query_id).duration
      }

      // Execute the query, we don't care about the results
      SQLPipeline sql_pipeline{sql}.get_result_table();
    }
  }
};

void create_report(const BenchmarkResults& results) {

}

void tpch_benchmark(BenchmarkMode benchmark_mode,
                    const opossum::ChunkOffset chunk_size,
                    const float scale_factor,
                    const size_t max_num_query_runs,
                    const std::chrono::steady_clock::rep seconds_per_query,
                    const std::vector<size_t>& query_ids) {
  /**
   * Populate the StorageManager
   */
  std::cout << "Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;
  opossum::TpchDbGenerator(scale_factor, chunk_size).generate_and_store();
  std::cout << "Done." << std::endl;

  BenchmarkResults query_results_by_query_id;

  // Run each requested query
  for (const auto query_id : query_ids) {
    auto num_runs = size_t{0};
    const auto query_benchmark_begin = std::chrono::steady_clock::now();
    auto query_benchmark_end = std::chrono::steady_clock::time_point{};
    const auto sql = opossum::tpch_queries[query_id];

    // Exercise a specific query, i.e. translate, optimize and run it
    while (true) {
      SQLPipeline sql_pipeline{sql};

      // Execute the query, we don't care about the results
      sql_pipeline.get_result_table();

      /**
       * Stop executing this query if `num_query_runs` runs of it have been performed or `seconds_per_query` passed
       */
      ++num_runs;
      if (num_runs >= max_num_query_runs) {
        query_benchmark_end = std::chrono::steady_clock::now();
        break;
      }

      /**
       * Stop exercising this query if sufficient time passed
       */
      {
        query_benchmark_end = std::chrono::steady_clock::now();
        const auto query_benchmark_seconds = std::chrono::duration_cast<std::chrono::seconds>(query_benchmark_end - query_benchmark_begin).count();
        if (query_benchmark_seconds >= seconds_per_query) {
          break;
        }
      }
    }

    QueryBenchmarkResult query_results{query_id};
    query_results.duration = query_benchmark_end - query_benchmark_begin;
    query_results.num_iterations = num_runs;

    query_results_by_query_id.emplace(query_id, query_results);
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
    ("s,scale", "Database scale factor (1.0 ~ 1GB)", cxxopts::value<float>(scale_factor)->default_value("1.0"))
    ("r,runs", "Minimum number of runs of a single query", cxxopts::value<size_t>(num_runs)->default_value("2"))
    ("c,chunk_size", "ChunkSize, defaults is 2^32-1", cxxopts::value<opossum::ChunkOffset>(chunk_size)->default_value(std::to_string(opossum::INVALID_CHUNK_OFFSET)))
    ("t,time", "Minimum time spend exercising a query", cxxopts::value<size_t>(time_seconds)->default_value("60"))
    ;

  const auto cli_parse_result = cli_options_description.parse(argc, argv);

  if (cli_parse_result.count("help")) {
    std::cout << cli_options_description.help({"", "Group"}) << std::endl;
    return 0;
  }

  tpch_benchmark(chunk_size, scale_factor, num_runs, time_seconds, {6,7});

  return 0;
}