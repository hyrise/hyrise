#include <memory>

#include "benchmark/benchmark.h"

#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "mvcc_benchmark_fixture.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {


BENCHMARK_DEFINE_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)(benchmark::State& state) {
  std::cout << "BM_MVCC_UPDATE" << "\n";
  //_incrementAllValuesByOne();

  int cnt = 0;

  for(auto _ : state) {
    cnt++;
    _clear_cache();
    auto get_table = std::make_shared<GetTable>(_table_name);
    get_table->execute();

    auto transaction_context = TransactionManager::get().new_transaction_context();
    auto validate_table = std::make_shared<Validate>(get_table);
    validate_table->set_transaction_context(transaction_context);
    validate_table->execute();

  }

  std::cout << "Finished run -> cnt = " << cnt << "\n";
}

// Run benchmark with a table of up to 100.000 invalidated lines
BENCHMARK_REGISTER_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)->RangeMultiplier(10)->Range(1, 100000); // ->Range(1, 80000);

}  // namespace opossum
