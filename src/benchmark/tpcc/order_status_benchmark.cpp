#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/limit.hpp"
#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCOrderStatusBenchmark : public TPCCBenchmarkFixture {
 public:
  std::vector<std::shared_ptr<OperatorTask>> get_customer_by_name(const std::string c_last, const int c_d_id,
                                                                  const int c_w_id) {
    /**
     * SELECT c_balance, c_first, c_middle, c_id
     * FROM customer
     * WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
     * ORDER BY c_first;
     */
    auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt_customer, "C_LAST", "=", c_last);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    std::vector<std::string> columns = {"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_ID"};
    auto projection = std::make_shared<Projection>(third_filter, columns);
    auto sort = std::make_shared<Sort>(projection, "C_FIRST", true);

    auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
    auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
    auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
    auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
    auto projection_task = std::make_shared<OperatorTask>(projection);
    auto sort_task = std::make_shared<OperatorTask>(sort);

    gt_customer_task->set_as_predecessor_of(first_filter_task);
    first_filter_task->set_as_predecessor_of(second_filter_task);
    second_filter_task->set_as_predecessor_of(third_filter_task);
    third_filter_task->set_as_predecessor_of(projection_task);
    projection_task->set_as_predecessor_of(sort_task);

    return {gt_customer_task, first_filter_task, second_filter_task, third_filter_task, projection_task, sort_task};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) {
    /**
     * SQL SELECT c_balance, c_first, c_middle, c_last
     * FROM customer
     * WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
     */
    auto gt_customer = std::make_shared<GetTable>("CUSTOMER");
    auto first_filter = std::make_shared<TableScan>(gt_customer, "C_ID", "=", c_id);
    auto second_filter = std::make_shared<TableScan>(first_filter, "C_D_ID", "=", c_d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "C_W_ID", "=", c_w_id);
    std::vector<std::string> columns = {"C_BALANCE", "C_FIRST", "C_MIDDLE", "C_LAST"};
    auto projection = std::make_shared<Projection>(third_filter, columns);

    auto gt_customer_task = std::make_shared<OperatorTask>(gt_customer);
    auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
    auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
    auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
    auto projection_task = std::make_shared<OperatorTask>(projection);

    gt_customer_task->set_as_predecessor_of(first_filter_task);
    first_filter_task->set_as_predecessor_of(second_filter_task);
    second_filter_task->set_as_predecessor_of(third_filter_task);
    third_filter_task->set_as_predecessor_of(projection_task);

    return {gt_customer_task, first_filter_task, second_filter_task, third_filter_task, projection_task};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_orders() {
    /**
     * SELECT o_id, o_carrier_id, o_entry_d
     * FROM orders
     * ORDER BY o_id DESC
     * LIMIT 1;
     */
    auto gt_orders = std::make_shared<GetTable>("ORDER");
    std::vector<std::string> columns = {"O_ID", "O_CARRIER_ID", "O_ENTRY_D"};
    auto projection = std::make_shared<Projection>(gt_orders, columns);
    auto sort = std::make_shared<Sort>(projection, "O_ID", false);
    auto limit = std::make_shared<Limit>(sort, 1);

    auto gt_orders_task = std::make_shared<OperatorTask>(gt_orders);
    auto projection_task = std::make_shared<OperatorTask>(projection);
    auto sort_task = std::make_shared<OperatorTask>(sort);
    auto limit_task = std::make_shared<OperatorTask>(limit);

    gt_orders_task->set_as_predecessor_of(projection_task);
    projection_task->set_as_predecessor_of(sort_task);
    sort_task->set_as_predecessor_of(limit_task);

    return {gt_orders_task, projection_task, sort_task, limit_task};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_order_lines(const int o_id, const int d_id, const int w_id) {
    /**
     * SELECT ol_i_id, ol_supply_w_id, ol_quantity,
     * ol_amount, ol_delivery_d
     * FROM order_line
     * WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
     */
    auto gt_order_lines = std::make_shared<GetTable>("ORDER-LINE");
    auto first_filter = std::make_shared<TableScan>(gt_order_lines, "OL_O_ID", "=", o_id);
    auto second_filter = std::make_shared<TableScan>(first_filter, "OL_D_ID", "=", d_id);
    auto third_filter = std::make_shared<TableScan>(second_filter, "OL_W_ID", "=", w_id);
    std::vector<std::string> columns = {"OL_I_ID", "OL_SUPPLY_W_ID", "OL_QUANTITY", "OL_AMOUNT", "OL_DELIVERY_D"};
    auto projection = std::make_shared<Projection>(third_filter, columns);

    auto gt_order_lines_task = std::make_shared<OperatorTask>(gt_order_lines);
    auto first_filter_task = std::make_shared<OperatorTask>(first_filter);
    auto second_filter_task = std::make_shared<OperatorTask>(second_filter);
    auto third_filter_task = std::make_shared<OperatorTask>(third_filter);
    auto projection_task = std::make_shared<OperatorTask>(projection);

    gt_order_lines_task->set_as_predecessor_of(first_filter_task);
    first_filter_task->set_as_predecessor_of(second_filter_task);
    second_filter_task->set_as_predecessor_of(third_filter_task);
    third_filter_task->set_as_predecessor_of(projection_task);

    return {gt_order_lines_task, first_filter_task, second_filter_task, third_filter_task, projection_task};
  }
};

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetCustomerByName)(benchmark::State &state) {
  clear_cache();
  auto c_last = _random_gen.last_name(2000);
  auto c_d_id = _random_gen.number(1, 10);
  auto c_w_id = 0;  // there is only one warehouse

  while (state.KeepRunning()) {
    auto get_customer_tasks = get_customer_by_name(c_last, c_d_id, c_w_id);
    schedule_tasks_and_wait(get_customer_tasks);
  }
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetCustomerById)(benchmark::State &state) {
  clear_cache();
  auto c_last = _random_gen.last_name(2000);
  auto c_d_id = _random_gen.number(1, 10);
  auto c_w_id = 0;  // there is only one warehouse
  auto c_id = _random_gen.nurand(1023, 1, 3000);

  while (state.KeepRunning()) {
    auto get_customer_tasks = get_customer_by_id(c_id, c_d_id, c_w_id);
    schedule_tasks_and_wait(get_customer_tasks);
  }
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetOrder)(benchmark::State &state) {
  clear_cache();
  auto c_last = _random_gen.last_name(2000);

  while (state.KeepRunning()) {
    auto get_order_tasks = get_orders();
    schedule_tasks_and_wait(get_order_tasks);
  }
}

// skip due to long execution times
BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus_GetOrderLine)(benchmark::State &state) {
  clear_cache();
  auto c_last = _random_gen.last_name(2000);
  auto c_d_id = _random_gen.number(1, 10);
  auto c_w_id = 0;  // there is only one warehouse

  while (state.KeepRunning()) {
    auto get_order_line_tasks = get_order_lines(0, c_d_id, c_w_id);
    schedule_tasks_and_wait(get_order_line_tasks);
  }
}

BENCHMARK_F(TPCCOrderStatusBenchmark, BM_TPCC_OrderStatus)(benchmark::State &state) {
  clear_cache();

  while (state.KeepRunning()) {
    // pass in i>1000 to trigger random value generation
    auto c_last = _random_gen.last_name(2000);
    auto c_d_id = _random_gen.number(1, 10);
    auto c_w_id = 0;  // there is only one warehouse
    auto c_id = _random_gen.nurand(1023, 1, 3000);

    // query by last name 6 out of 10 times
    if (_random_gen.number(0, 10) < 6) {
      auto get_customer_tasks = get_customer_by_name(c_last, c_d_id, c_w_id);
      schedule_tasks_and_wait(get_customer_tasks);

      auto num_names = get_customer_tasks.back()->get_operator()->get_output()->row_count();
      assert(num_names > 0);

      auto customer =
          get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), ceil(num_names / 2));
    } else {
      auto get_customer_tasks = get_customer_by_id(c_id, c_d_id, c_w_id);
      schedule_tasks_and_wait(get_customer_tasks);

      assert(get_customer_tasks.back()->get_operator()->get_output()->row_count() == 1);
      auto customer = get_from_table_at_row(get_customer_tasks.back()->get_operator()->get_output(), 0);
    }

    auto get_order_tasks = get_orders();
    schedule_tasks_and_wait(get_order_tasks);

    auto get_order_line_tasks = get_order_lines(0, c_d_id, c_w_id);
    schedule_tasks_and_wait(get_order_line_tasks);
  }
}

}  // namespace opossum
