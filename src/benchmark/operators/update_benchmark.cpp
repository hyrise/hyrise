#include <memory>

#include "benchmark/benchmark.h"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "storage/table.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Update)(benchmark::State& state) {
  _clear_cache();
  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "a");
  const auto table = load_table("src/test/tables/int_float2.tbl", 100);
  StorageManager::get().add_table("update_table", table);
  
  const auto get_table = std::make_shared<GetTable>("update_table");
  const auto where_scan = std::make_shared<TableScan>(get_table, expression_functional::equals_(column, 1));
  const auto updated_values_projection = std::make_shared<Projection>(where_scan, expression_functional::expression_vector(column, 1));
  get_table->execute();
  where_scan->execute();
  updated_values_projection->execute();

  while (state.KeepRunning()) {
    const auto transaction_context = TransactionManager::get().new_transaction_context();
    const auto update = std::make_shared<Update>("update_table", where_scan, updated_values_projection);
    update->set_transaction_context(transaction_context);
    update->execute();
    transaction_context->commit();
  }
  StorageManager::reset();
}

}  // namespace opossum
