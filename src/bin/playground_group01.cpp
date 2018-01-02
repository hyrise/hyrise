#include <chrono>
#include <iostream>

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tuning/index_tuner.hpp"
#include "tuning/system_statistics.hpp"

using std::chrono::high_resolution_clock;

// Forward declarations
int _execute_sample_queries(opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache);

int main() {
  opossum::SQLQueryCache<opossum::SQLQueryPlan> cache(1024);
  auto statistics = std::make_shared<opossum::SystemStatistics>(cache);
  opossum::IndexTuner tuner(statistics);

  // Generate TPCC tables
  // -- This would generate all tables
  //  auto tables = tpcc::TpccTableGenerator().generate_all_tables();
  //  for (auto& pair : tables) {
  //    opossum::StorageManager::get().add_table(pair.first, pair.second);
  //  }
  // -- This will generate only the customer table
  auto customer_table = tpcc::TpccTableGenerator().generate_customer_table();
  opossum::StorageManager::get().add_table("CUSTOMER", customer_table);

  // Fire SQL query and cache it
  auto first_execution_time = _execute_sample_queries(cache);

  // Let the tuner optimize tables based on the values of the cache
  tuner.execute();

  // Execute the same queries a second time and measure the speedup
  auto second_execution_time = _execute_sample_queries(cache);

  float percentage = (static_cast<float>(second_execution_time) / static_cast<float>(first_execution_time));
  percentage *= 100;

  std::cout << "Execution times (microseconds):\n";
  std::cout << "  Before tuning: " << first_execution_time << "\n";
  std::cout << "  After tuning:  " << second_execution_time << "\n";
  std::cout << "                 (" << percentage << "%)\n";

  return 0;
}

// Executes some sample queries and manually stores them in the cache
// Returns the execution time in microseconds
int _execute_sample_queries(opossum::SQLQueryCache<opossum::SQLQueryPlan>& cache) {
  //  auto op = std::make_shared<opossum::SQLQueryOperator>("SELECT * FROM CUSTOMER WHERE C_DISCOUNT > 0.3 LIMIT 20");
  //  auto task = std::make_shared<opossum::OperatorTask>(op);
  //  task->execute();

  const std::string query = "SELECT * FROM CUSTOMER WHERE C_DISCOUNT > 0.3 LIMIT 20";
  opossum::SQLPipeline pipeline(query);
  const auto& lqps = pipeline.get_optimized_logical_plans();

  for (const auto& lqp : lqps) {
    lqp->print();
  }

  cache.set(query, pipeline.get_query_plan());

  // ToDo(group01): Discuss which method calls to measure
  high_resolution_clock::time_point start_time = high_resolution_clock::now();

  pipeline.get_result_table();

  high_resolution_clock::time_point end_time = high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
  return static_cast<int>(duration);
}
