#include <iostream>

#include "concurrency/transaction_manager.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_query_operator.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "tuning/index_tuner.hpp"

int main() {
  opossum::IndexTuner tuner;

  // Generate TPCC tables
  auto tables = tpcc::TpccTableGenerator().generate_all_tables();
  for (auto& pair : tables) {
    opossum::StorageManager::get().add_table(pair.first, pair.second);
  }

  // why not
  opossum::TransactionManager::reset();
  opossum::TransactionManager::new_transaction_context();

  // Fire SQL query
  auto op = std::make_shared<opossum::SQLQueryOperator>("SELECT * FROM CUSTOMER WHERE C_DISCOUNT > 0.3 LIMIT 20");
  auto task = std::make_shared<opossum::OperatorTask>(op);
  task->execute();

  //opossum::SQLPipeline pipeline("SELECT * FROM CUSTOMER WHERE C_DISCOUNT > 0.3 LIMIT 20;");
  //const auto & query_plan = pipeline.get_query_plan();
  //query_plan.num_parameters();
  //pipeline.get_result_table();

  tuner.execute();

  return 0;
}
