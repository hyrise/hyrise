#include <chrono>
#include <iostream>

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
  std::cout << "Generating TPCH Tables with scale_factor=" << scale_factor;
  opossum::TpchDbGenerator(scale_factor, chunk_size).generate_and_store();
  std::cout << std::endl;

  /**
   * Run each requested query and report performance statistics.
   */
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

  boost::program_options::options_description options_description("Allowed options");
  options_description.add_options()
    ("help", "print this help message")
    ("scale,s", boost::program_options::value<float>(&scale_factor)->default_value(1.0f), "Database scale factor (1.0 ~ 1GB)")
    ("runs,r", boost::program_options::value<size_t>(&num_runs)->default_value(2), "Minimum number of runs of a single query")
    ("chunk_size,c", boost::program_options::value<opossum::ChunkOffset>(&chunk_size)->default_value(opossum::INVALID_CHUNK_OFFSET), "Chunk Size")
    ("time,t", boost::program_options::value<size_t>(&time_seconds)->default_value(60), "Minimum time spend exercising a query");

  boost::program_options::variables_map program_options;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, options_description), program_options);
  boost::program_options::notify(program_options);

  if (program_options.count("help")) {
    std::cout << options_description << "\n";
    return 1;
  }

  tpch_benchmark(100, 0.01f, 2, 40, {6, 7});

  return 0;
}