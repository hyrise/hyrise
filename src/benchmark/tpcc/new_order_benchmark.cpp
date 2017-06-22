#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "concurrency/transaction_manager.hpp"
#include "operators/commit_records.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "scheduler/operator_task.hpp"

#include "tpcc/constants.hpp"
#include "tpcc/new_order.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {
 public:
  void benchmark_new_order(benchmark::State &state, tpcc::AbstractNewOrderImpl &impl) {
    clear_cache();

    while (state.KeepRunning()) {
      NewOrderParams params;

      params.w_id = 0;  // TODO(anybody): Change when implementing multiple warehouses
      params.d_id = _random_gen.number(0, NUM_DISTRICTS_PER_WAREHOUSE - 1);
      params.c_id = _random_gen.number(0, NUM_CUSTOMERS_PER_DISTRICT - 1);
      params.o_entry_d = 0;  // TODO(anybody): Real data
      params.order_lines.reserve(_random_gen.number(MIN_ORDER_LINE_COUNT, MAX_ORDER_LINE_COUNT));

      for (size_t ol = 0; ol < params.order_lines.size(); ol++) {
        auto & order_line = params.order_lines[ol];
        order_line.w_id = 0;  // TODO(anybody): Change when implementing multiple warehouses
        order_line.i_id = _random_gen.number(0, NUM_ITEMS - 1);
        order_line.qty = _random_gen.number(1, MAX_ORDER_LINE_QUANTITY);
      }

      impl.run_transaction(params);
    }
  }

 protected:
  NewOrderRefImpl _ref_impl;
};

BENCHMARK_F(TPCCNewOrderBenchmark, BM_TPCC_NewOrder)(benchmark::State& state) {
  benchmark_new_order(state, _ref_impl);
}

}  // namespace opossum
