#include <chrono>
#include <iostream>
#include <memory>

#include "concurrency/transaction_manager.hpp"
#include "operators/import_binary.hpp"
#include "optimizer/column_statistics.hpp"
#include "optimizer/table_statistics.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tuning/greedy_selector.hpp"
#include "tuning/index/index_evaluator.hpp"
#include "tuning/tuner.hpp"
#include "utils/assert.hpp"
#include "utils/logging.hpp"

using std::chrono::high_resolution_clock;

// Test set of queries - for development.
const std::vector<std::string> test_queries{"SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Danni Cohdwell'",
                                            "SELECT NAME FROM CUSTOMER WHERE LEVEL = 5",
                                            "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Yvonne Tonneson'",
                                            "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Ethelda Granny'",
                                            "SELECT NAME FROM CUSTOMER WHERE LEVEL = 3",
                                            "SELECT INTEREST FROM CUSTOMER WHERE NAME  = 'Rosemary Picardi'",
                                            "SELECT BALANCE FROM CUSTOMER WHERE NAME = 'Danni Cohdwell'"};

// Forward declarations
std::shared_ptr<opossum::SQLPipeline> _create_and_cache_pipeline(const std::string& query);
int _execute_query(const std::string& query, unsigned int execution_count);

int main() {
  opossum::Tuner tuner;
  tuner.add_evaluator(std::make_unique<opossum::IndexEvaluator>());
  tuner.set_selector(std::make_unique<opossum::GreedySelector>());

  LOG_INFO("Loading binary table...");
  auto importer = std::make_shared<opossum::ImportBinary>("group01_CUSTOMER.bin", "CUSTOMER");
  importer->execute();
  LOG_INFO("Table loaded.\n");

  LOG_INFO("Request table statistics.");
  auto table = opossum::StorageManager::get().get_table("CUSTOMER");
  const auto& statistics = table->table_statistics()->column_statistics();
  statistics.size();
  LOG_INFO("Table statistics (" << statistics.at(1)->distinct_count() << ", " << statistics.at(3)->distinct_count()
                                << ")");

  constexpr unsigned int execution_count = 5;

  std::vector<int> first_execution_times(test_queries.size());
  std::vector<int> second_execution_times(test_queries.size());

  LOG_INFO("Executing queries a first time to fill up the cache...");
  // Fire SQL query and cache it
  for (auto query_index = 0u; query_index < test_queries.size(); ++query_index) {
    LOG_DEBUG("  -> " << query_index + 1 << "/" << test_queries.size() << ": " << test_queries[query_index]);
    first_execution_times[query_index] = _execute_query(test_queries[query_index], execution_count);
  }
  LOG_INFO("Queries executed.\n");

  // Let the tuner optimize tables based on the values of the cache
  LOG_INFO("Executing IndexTuner...");
  tuner.schedule_tuning_process();
  tuner.wait_for_completion();
  LOG_INFO("IndexTuner executed.\n");

  LOG_INFO("Executing queries a second time (with optimized indices)...");
  LOG_INFO("Execution times (microseconds):");

  // Execute the same queries a second time and measure the speedup
  for (auto query_index = 0u; query_index < test_queries.size(); ++query_index) {
    second_execution_times[query_index] = _execute_query(test_queries[query_index], execution_count);

    float percentage = (static_cast<float>(second_execution_times[query_index]) /
                        static_cast<float>(first_execution_times[query_index])) *
                       100.0;
    LOG_INFO("Query: " << test_queries[query_index] << " reduced to: " << percentage);
    LOG_DEBUG("  before/after: " << first_execution_times[query_index] << " / " << second_execution_times[query_index]);
  }

  LOG_INFO("Executing IndexTuner AGAIN (sanity check)...");
  tuner.schedule_tuning_process();
  tuner.wait_for_completion();

  LOG_INFO("IndexTuner executed a second time.");
  return 0;
}

// Creates a Pipeline based on the supplied query and puts its query plan in the supplied cache
std::shared_ptr<opossum::SQLPipeline> _create_and_cache_pipeline(const std::string& query) {
  auto pipeline = std::make_shared<opossum::SQLPipeline>(query, opossum::UseMvcc::No);

  auto query_plans = pipeline->get_query_plans();

  // ToDo(group01): What is the semantics of multiple entries per query? Handle cases accordingly.
  opossum::Assert(query_plans.size() == 1, "Expected only one query plan per pipeline");
  return pipeline;
}

// Executes a query repeatedly and measures the execution time
int _execute_query(const std::string& query, unsigned int execution_count) {
  int accumulated_duration = 0;

  // Execute queries multiple times to get more stable timing results
  for (auto counter = 0u; counter < execution_count; counter++) {
    auto pipeline = _create_and_cache_pipeline(query);
    pipeline->get_result_table();
    accumulated_duration += pipeline->execution_time_microseconds().count();
  }

  return accumulated_duration / execution_count;
}
