#include <chrono>
#include <iostream>

#include "cxxopts.hpp"

#include <boost/program_options.hpp>
#include <SQLParserResult.h>
#include <SQLParser.h>

#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"

namespace {

void tpch_benchmark(const opossum::ChunkOffset chunk_size,
                    const float scale_factor,
                    const size_t max_num_query_runs,
                    const std::chrono::steady_clock::rep seconds_per_query,
                    std::vector<size_t> query_ids) {
  /**
   * Populate the StorageManager
   */
  std::cout << "Generating TPCH Tables with scale_factor=" << scale_factor << "..." << std::endl;
  opossum::TpchDbGenerator(scale_factor, chunk_size).generate_and_store();
  std::cout << "Done." << std::endl;

  // Run each requested query
  for (const auto query_id : query_ids) {
    auto num_runs = size_t{0};
    const auto query_benchmark_begin = std::chrono::steady_clock::now();
    const auto sql = opossum::tpch_queries[query_id];

    while (true) {
      hsql::SQLParserResult parser_result;
      hsql::SQLParser::parse(sql, &parser_result);

      /**
       * Stop executing this query if `num_query_runs` runs of it have been performed or `seconds_per_query` passed
       */
      ++num_runs;
      if (num_runs >= max_num_query_runs) {
        break;
      }

      /**
       * Stop exercising this query, if sufficient time passed
       */
      {
        const auto now = std::chrono::steady_clock::now();
        const auto query_benchmark_seconds = std::chrono::duration_cast<std::chrono::seconds>(now - query_benchmark_begin).count();
        if (query_benchmark_seconds >= seconds_per_query) {
          break;
        }
      }

    }

  }
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