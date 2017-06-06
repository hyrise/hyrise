#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "concurrency/transaction_manager.hpp"
#include "operators/commit_records.hpp"
#include "operators/get_table.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {

};

BENCHMARK_F(TPCCNewOrderBenchmark, BM_TPCC_NewOrder)(benchmark::State& state) {
  clear_cache();

  while (state.KeepRunning()) {
  }
}

}  // namespace opossum
