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

BENCHMARK_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)(benchmark::State& state) {

  // TODO Update some values or just do a table scan in order to iterate across the table with its invalidated lines

  // Preparation
  const auto get_table = std::make_shared<GetTable>(_table_name);

}


// Run benchmark with using a table with up to 80000 invalidated lines
BENCHMARK_REGISTER_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)->RangeMultiplier(2)->Range(1, 80000);

}  // namespace opossum
