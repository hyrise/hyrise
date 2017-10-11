#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "tpcc_base_fixture.hpp"

#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/print.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "scheduler/operator_task.hpp"

#include "tpcc/helper.hpp"
#include "tpcc/order_status.hpp"

namespace opossum {

class TPCCOrderStatusBenchmark : public TPCCBenchmarkFixture {
 public:
  void benchmark_get_customer_by_name(benchmark::State& state, tpcc::AbstractOrderStatusImpl& impl) {
    clear_cache();
    auto c_last = _random_gen.last_name(2000);
    auto c_d_id = _random_gen.random_number(1, 10);
    auto c_w_id = _random_gen.random_number(0, _gen._warehouse_size - 1);

    while (state.KeepRunning()) {
      TransactionManager::get().run_transaction([&](std::shared_ptr<TransactionContext> t_context) {
        auto get_customer_tasks = impl.get_customer_by_name(c_last, c_d_id, c_w_id);
        tpcc::execute_tasks_with_context(get_customer_tasks, t_context);
      });
    }
  }

  void benchmark_get_customer_by_id(benchmark::State& state, tpcc::AbstractOrderStatusImpl& impl) {
    clear_cache();
    auto c_last = _random_gen.last_name(2000);
    auto c_d_id = _random_gen.random_number(1, 10);
    auto c_w_id = _random_gen.random_number(0, _gen._warehouse_size - 1);
    auto c_id = _random_gen.nurand(1023, 1, 3000);

    while (state.KeepRunning()) {
      TransactionManager::get().run_transaction([&](std::shared_ptr<TransactionContext> t_context) {
        auto get_customer_tasks = impl.get_customer_by_id(c_id, c_d_id, c_w_id);
        tpcc::execute_tasks_with_context(get_customer_tasks, t_context);
      });
    }
  }

  void benchmark_get_order(benchmark::State& state, tpcc::AbstractOrderStatusImpl& impl) {
    clear_cache();

    auto c_d_id = _random_gen.random_number(1, 10);
    auto c_w_id = _random_gen.random_number(0, _gen._warehouse_size - 1);
    auto c_id = _random_gen.nurand(1023, 1, 3000);

    while (state.KeepRunning()) {
      TransactionManager::get().run_transaction([&](std::shared_ptr<TransactionContext> t_context) {
        auto get_order_tasks = impl.get_orders(c_id, c_d_id, c_w_id);
        tpcc::execute_tasks_with_context(get_order_tasks, t_context);
      });
    }
  }

  void benchmark_get_order_line(benchmark::State& state, tpcc::AbstractOrderStatusImpl& impl) {
    clear_cache();
    auto c_last = _random_gen.last_name(2000);
    auto c_d_id = _random_gen.random_number(1, 10);
    auto c_w_id = _random_gen.random_number(0, _gen._warehouse_size - 1);

    while (state.KeepRunning()) {
      TransactionManager::get().run_transaction([&](std::shared_ptr<TransactionContext> t_context) {
        auto get_order_line_tasks = impl.get_order_lines(0, c_d_id, c_w_id);
        tpcc::execute_tasks_with_context(get_order_line_tasks, t_context);
      });
    }
  }

  void benchmark_order_status(benchmark::State& state, tpcc::AbstractOrderStatusImpl& impl) {
    clear_cache();

    while (state.KeepRunning()) {
      tpcc::OrderStatusParams params;

      params.c_w_id = 0;  // there is only one warehouse
      params.c_d_id = _random_gen.random_number(1, 10);

      if (_random_gen.random_number(0, 10) < 6) {
        params.order_status_by = tpcc::OrderStatusBy::CustomerLastName;
        params.c_last = _random_gen.last_name(2000);  // pass in i>1000 to trigger random value generation
      } else {
        params.order_status_by = tpcc::OrderStatusBy::CustomerNumber;
        params.c_id = _random_gen.nurand(1023, 1, 3000);
      }

      impl.run_transaction(params);
    }
  }

 protected:
  tpcc::OrderStatusRefImpl _ref_impl;
};

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetCustomerByName)(benchmark::State& state) {
  benchmark_get_customer_by_name(state, _ref_impl);
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetCustomerById)(benchmark::State& state) {
  benchmark_get_customer_by_id(state, _ref_impl);
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetOrder)(benchmark::State& state) {
  benchmark_get_order(state, _ref_impl);
}

// skip due to long execution times
BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetOrderLine)(benchmark::State& state) {
  benchmark_get_order_line(state, _ref_impl);
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus)(benchmark::State& state) {
  benchmark_order_status(state, _ref_impl);
}

}  // namespace opossum
