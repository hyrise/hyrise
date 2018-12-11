#include <memory>

#include "benchmark/benchmark.h"
#include "concurrency/transaction_manager.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "mvcc_benchmark_fixture.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

BENCHMARK_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)(benchmark::State& state) {
  _clear_cache();
  _incrementAllValuesByOne();
}

// Run benchmark with a table of up to 80000 invalidated lines
BENCHMARK_REGISTER_F(MVCC_Benchmark_Fixture, BM_MVCC_UPDATE)
    ->RangeMultiplier(2)
    ->Range(1, 80000);  // ->Range(1 << 8, 1 << 30);

}  // namespace opossum
