#include <iostream>

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_cache.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tuning/index_tuner.hpp"
#include "tuning/system_statistics.hpp"

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

  // Fire SQL query
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

  pipeline.get_result_table();

  tuner.execute();

  return 0;
}
