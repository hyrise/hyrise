#include <chrono>
#include <iostream>

#include "concurrency/transaction_manager.hpp"
#include "operators/import_binary.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tuning/index_tuner.hpp"
#include "tuning/system_statistics.hpp"

using std::chrono::high_resolution_clock;

// Test set of queries - for development.
// ToDo(group01): as soon as caching is integrated into the SQLPipeline, we should run a bigger and more standardized
//                workload, e.g. the TPC-C benchmark
// Idea behind the current queries: have three indexable columns, but one only used once, one twice, and one thrice.
std::vector<std::string> test_queries{
    "SELECT * FROM CUSTOMER WHERE INTEREST > 0.3",         "SELECT * FROM CUSTOMER WHERE INTEREST < 0.6",
    "SELECT * FROM CUSTOMER WHERE NAME = 'BARBARBAR'",     "SELECT * FROM CUSTOMER WHERE NAME = 'OUGHTPRIBAR'",
    "SELECT * FROM CUSTOMER WHERE NAME = 'OUGHTANTIPRES'", "SELECT * FROM CUSTOMER WHERE LEVEL = 3"};

// Forward declarations
std::shared_ptr<opossum::SQLPipeline> _create_and_cache_pipeline(const std::string& query,
                                                                 opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache);
int _execute_query(const std::string& query, unsigned int execution_count,
                   opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache);

int main() {
  opossum::SQLQueryCache<opossum::SQLQueryPlan> cache(1024);
  auto statistics = std::make_shared<opossum::SystemStatistics>(cache);
  opossum::IndexTuner tuner(statistics);

  auto importer = std::make_shared<opossum::ImportBinary>("group01_CUSTOMER.bin", "CUSTOMER");
  importer->execute();

  constexpr unsigned int execution_count = 5;

  std::vector<int> first_execution_times(test_queries.size());
  std::vector<int> second_execution_times(test_queries.size());

  // Fire SQL query and cache it
  for (auto query_index = 0u; query_index < test_queries.size(); ++query_index) {
    first_execution_times[query_index] = _execute_query(test_queries[query_index], execution_count, cache);
  }

  // Let the tuner optimize tables based on the values of the cache
  tuner.execute();

  std::cout << "Execution times (microseconds):\n";

  // Execute the same queries a second time and measure the speedup
  for (auto query_index = 0u; query_index < test_queries.size(); ++query_index) {
    second_execution_times[query_index] = _execute_query(test_queries[query_index], execution_count, cache);

    float percentage = (static_cast<float>(second_execution_times[query_index]) /
                        static_cast<float>(first_execution_times[query_index])) *
                       100.0;
    std::cout << "Query " << query_index << ": " << percentage << "% (" << first_execution_times[query_index] << " / "
              << second_execution_times[query_index] << ")\n";
  }

  return 0;
}

// Creates a Pipeline based on the supplied query and puts its query plan in the supplied cache
std::shared_ptr<opossum::SQLPipeline> _create_and_cache_pipeline(const std::string& query,
                                                                 opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache) {
  auto pipeline = std::make_shared<opossum::SQLPipeline>(query);

  cache.set(query, pipeline->get_query_plan());
  return pipeline;
}

// Executes a query repeatedly and measures the execution time
int _execute_query(const std::string& query, unsigned int execution_count,
                   opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache) {
  int accumulated_duration = 0;

  // Execute queries multiple times to get more stable timing results
  for (auto counter = 0u; counter < execution_count; counter++) {
    auto pipeline = _create_and_cache_pipeline(query, cache);
    pipeline->get_result_table();
    accumulated_duration += pipeline->execution_time_microseconds().count();
  }

  return accumulated_duration / execution_count;
}
