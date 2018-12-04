#include <memory>

#include "benchmark/benchmark.h"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "mvcc_benchmark_fixture.h"
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

void queryTable(std::string& tableName) {

  // Preparation
  const auto get_table = std::make_shared<GetTable>(tableName);
  const auto where_scan = std::make_shared<TableScan>(get_table, expression_functional::equals_(column, 1));
  get_table->execute();

  auto column = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "column0");
}


void applyUpdatesToTable(std::string& tableName) {

  const auto updated_values_projection = std::make_shared<Projection>(where_scan, expression_functional::expression_vector(column, 1));

  where_scan->execute();
  updated_values_projection->execute();

  while (state.KeepRunning()) {

    const auto transaction_context = TransactionManager::get().new_transaction_context();
    const auto update = std::make_shared<Update>("update_table", where_scan, updated_values_projection);
    update->set_transaction_context(transaction_context);
    update->execute();
    transaction_context->commit();
  }

}



BENCHMARK_F(MVCC_Benchmark_Fixture, BM_Update)(benchmark::State& state) {

  const int updateCycles = 100;


  for (int i = 0; i < updateCycles; i++) {

      // Query table data
      _clear_cache();

      // TODO measure time ----->

      queryTable();

      // TODO <------------------


      // ##### Modify table #####

      applyUpdatesToTable(_table_name)



  }

}


}  // namespace opossum
