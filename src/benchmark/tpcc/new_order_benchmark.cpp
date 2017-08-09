#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "tpcc_base_fixture.hpp"

#include "tpcc/constants.hpp"
#include "tpcc/new_order.hpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {
 public:
  void benchmark_new_order(benchmark::State &state, tpcc::AbstractNewOrderImpl &impl) {
    clear_cache();

    while (state.KeepRunning()) {
      tpcc::NewOrderParams params;

      params.w_id = static_cast<int32_t>(_random_gen.random_number(0, _gen._warehouse_size - 1));
      params.d_id = static_cast<int32_t>(_random_gen.random_number(0, tpcc::NUM_DISTRICTS_PER_WAREHOUSE - 1));
      params.c_id = static_cast<int32_t>(_random_gen.random_number(0, tpcc::NUM_CUSTOMERS_PER_DISTRICT - 1));
      params.o_entry_d = 0;  // TODO(anybody): Real data
      params.order_lines.reserve(_random_gen.random_number(tpcc::MIN_ORDER_LINE_COUNT, tpcc::MAX_ORDER_LINE_COUNT));

      for (size_t ol = 0; ol < params.order_lines.size(); ol++) {
        auto &order_line = params.order_lines[ol];
        order_line.w_id = static_cast<int32_t>(_random_gen.random_number(0, _gen._warehouse_size - 1));
        order_line.i_id = static_cast<int32_t>(_random_gen.random_number(0, tpcc::NUM_ITEMS - 1));
        order_line.qty = static_cast<int32_t>(_random_gen.random_number(1, tpcc::MAX_ORDER_LINE_QUANTITY));
      }

      impl.run_transaction(params);
    }
  }

 protected:
  tpcc::NewOrderRefImpl _ref_impl;
};

BENCHMARK_F(TPCCNewOrderBenchmark, BM_TPCC_NewOrder)(benchmark::State &state) { benchmark_new_order(state, _ref_impl); }

}  // namespace opossum
