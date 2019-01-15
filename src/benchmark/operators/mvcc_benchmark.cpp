#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "storage/reference_segment.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_MVCC)(benchmark::State& state) {
  _clear_cache();

  const auto table = load_table("../src/test/tables/int_float2.tbl", 10);

  auto& storage_manager = StorageManager::get();

  storage_manager.add_table("benchmark_table", table);

  for (auto _ : state) {
    for (int i = 0; i < state.range(0); ++i) {
      const auto& column_a = expression_functional::pqp_column_(ColumnID{0}, DataType::Int, false, "a");

      const auto& transaction_context = TransactionManager::get().new_transaction_context();

      const auto get_table = std::make_shared<GetTable>("benchmark_table");
      const auto where_scan = std::make_shared<TableScan>(get_table, expression_functional::greater_than_(column_a, 0));

      get_table->set_transaction_context(transaction_context);
      get_table->execute();
      auto validate = std::make_shared<Validate>(get_table);
      validate->set_transaction_context(transaction_context);

      validate->execute();

      const auto updated_values_projection =
          std::make_shared<Projection>(validate, expression_functional::expression_vector(column_a, 1.5f));

      where_scan->set_transaction_context(transaction_context);
      where_scan->execute();
      updated_values_projection->set_transaction_context(transaction_context);
      updated_values_projection->execute();

      const auto& update = std::make_shared<Update>("benchmark_table", validate, updated_values_projection);
      update->set_transaction_context(transaction_context);
      update->execute();
      transaction_context->commit();
    }
  }
}
BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_MVCC)->RangeMultiplier(2)->Range(1 << 8, 1 << 30);
}  // namespace opossum
