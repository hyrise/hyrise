#include <cassert>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "concurrency/transaction_manager.hpp"
#include "operators/aggregate.hpp"
#include "operators/commit_records.hpp"
#include "operators/delete.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/sort.hpp"
#include "operators/table_scan.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

inline std::vector<std::shared_ptr<OperatorTask>> delivery_getDId(const int d_id, const int w_id) {
  /**
   * EXEC SQL DECLARE c_no CURSOR FOR
   * SELECT no_o_id
   * FROM new_order
   * WHERE no_d_id = :d_id AND no_w_id = :w_id ORDER BY no_o_id ASC;
   */
  auto gt = std::make_shared<GetTable>("NEW-ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("NO_D_ID"), "=", d_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("NO_W_ID"), "=", w_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("NO_O_ID"), ">", -1);
  auto val = std::make_shared<Validate>(ts3);
  auto projection = std::make_shared<Projection>(val, std::vector<std::string>{"NO_O_ID"});
  auto sort = std::make_shared<Sort>(projection, "NO_O_ID", true, 0);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_projection = std::make_shared<OperatorTask>(std::move(projection));
  auto t_sort = std::make_shared<OperatorTask>(std::move(sort));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_projection);
  t_projection->set_as_predecessor_of(t_sort);

  return {std::move(t_gt),  std::move(t_ts1),        std::move(t_ts2), std::move(t_ts3),
          std::move(t_val), std::move(t_projection), std::move(t_sort)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_del(const int no_o_id) {
  /**
   * EXEC SQL DELETE
   * FROM new_order
   * WHERE CURRENT OF c_no;
   */
  auto gt = std::make_shared<GetTable>("NEW-ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("NO_O_ID"), "=", no_o_id);
  auto val = std::make_shared<Validate>(ts1);
  auto delete_op = std::make_shared<Delete>("NEW-ORDER", val);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_delete_op = std::make_shared<OperatorTask>(std::move(delete_op));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_delete_op);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_val), std::move(t_delete_op)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_selOrder(const int d_id, const int w_id, const int no_o_id) {
  /**
   * EXEC SQL SELECT o_c_id INTO :c_id
   * FROM orders
   * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("O_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("O_W_ID"), "=", w_id);
  auto val = std::make_shared<Validate>(ts3);
  auto projection = std::make_shared<Projection>(val, std::vector<std::string>{"O_C_ID"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_projection = std::make_shared<OperatorTask>(std::move(projection));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_projection);

  return {std::move(t_gt),  std::move(t_ts1), std::move(t_ts2),
          std::move(t_ts3), std::move(t_val), std::move(t_projection)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateOrder(const int d_id, const int w_id,
                                                                       const int no_o_id, const int o_carrier_id) {
  /**
   * EXEC SQL UPDATE orders
   * SET o_carrier_id = :o_carrier_id
   * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("O_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("O_W_ID"), "=", w_id);
  auto val = std::make_shared<Validate>(ts3);
  auto projection = std::make_shared<Projection>(val, std::vector<std::string>{"O_CARRIER_ID"});
  Projection::ProjectionDefinitions definitions{
      Projection::ProjectionDefinition{std::to_string(o_carrier_id), "int", "O_CARRIER_ID"}};
  auto updated_rows = std::make_shared<Projection>(val, definitions);
  auto update = std::make_shared<Update>("ORDER", projection, updated_rows);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_projection = std::make_shared<OperatorTask>(std::move(projection));
  auto t_updated_rows = std::make_shared<OperatorTask>(std::move(updated_rows));
  auto t_update = std::make_shared<OperatorTask>(std::move(update));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_projection);
  t_val->set_as_predecessor_of(t_updated_rows);
  t_projection->set_as_predecessor_of(t_update);
  t_updated_rows->set_as_predecessor_of(t_update);

  return {std::move(t_gt),  std::move(t_ts1),        std::move(t_ts2),          std::move(t_ts3),
          std::move(t_val), std::move(t_projection), std::move(t_updated_rows), std::move(t_update)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateOrderLine(const int d_id, const int w_id,
                                                                           const int no_o_id, const time_t datetime) {
  /**
   * EXEC SQL UPDATE order_line
   * SET ol_delivery_d = :datetime
   * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER-LINE");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("OL_O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("OL_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("OL_W_ID"), "=", w_id);
  auto val = std::make_shared<Validate>(ts3);
  auto projection = std::make_shared<Projection>(val, std::vector<std::string>{"OL_DELIVERY_D"});
  Projection::ProjectionDefinitions definitions{
      Projection::ProjectionDefinition{std::to_string(datetime), "int", "OL_DELIVERY_D"}};
  auto updated_rows = std::make_shared<Projection>(val, definitions);
  auto update = std::make_shared<Update>("ORDER-LINE", projection, updated_rows);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_projection = std::make_shared<OperatorTask>(std::move(projection));
  auto t_updated_rows = std::make_shared<OperatorTask>(std::move(updated_rows));
  auto t_update = std::make_shared<OperatorTask>(std::move(update));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_projection);
  t_val->set_as_predecessor_of(t_updated_rows);
  t_projection->set_as_predecessor_of(t_update);
  t_updated_rows->set_as_predecessor_of(t_update);

  return {std::move(t_gt),  std::move(t_ts1),        std::move(t_ts2),          std::move(t_ts3),
          std::move(t_val), std::move(t_projection), std::move(t_updated_rows), std::move(t_update)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_sumOrderLine(const int d_id, const int w_id,
                                                                        const int no_o_id) {
  /**
   * EXEC SQL SELECT SUM(ol_amount) INTO :ol_total
   * FROM order_line
   * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER-LINE");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("OL_O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("OL_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("OL_W_ID"), "=", w_id);
  auto val = std::make_shared<Validate>(ts3);
  auto sum = std::make_shared<Aggregate>(
      val, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("OL_AMOUNT"), Sum)},
      std::vector<std::string>{});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_sum = std::make_shared<OperatorTask>(std::move(sum));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_sum);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_val), std::move(t_sum)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateCustomer(const double ol_total, const int d_id,
                                                                          const int w_id, const int c_id) {
  /**
   * EXEC SQL UPDATE customer
   * SET c_balance = c_balance + :ol_total
   * WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("CUSTOMER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("C_ID"), "=", c_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("C_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("C_W_ID"), "=", w_id);
  auto val = std::make_shared<Validate>(ts3);
  auto projection = std::make_shared<Projection>(val, std::vector<std::string>{"C_BALANCE"});
  Projection::ProjectionDefinitions definitions{
      Projection::ProjectionDefinition{"$C_BALANCE+" + std::to_string(ol_total), "float", "C_BALANCE"}};
  auto updated_rows = std::make_shared<Projection>(val, definitions);
  auto update = std::make_shared<Update>("CUSTOMER", projection, updated_rows);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_val = std::make_shared<OperatorTask>(std::move(val));
  auto t_projection = std::make_shared<OperatorTask>(std::move(projection));
  auto t_updated_rows = std::make_shared<OperatorTask>(std::move(updated_rows));
  auto t_update = std::make_shared<OperatorTask>(std::move(update));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_val);
  t_val->set_as_predecessor_of(t_projection);
  t_val->set_as_predecessor_of(t_updated_rows);
  t_projection->set_as_predecessor_of(t_update);
  t_updated_rows->set_as_predecessor_of(t_update);

  return {std::move(t_gt),  std::move(t_ts1),        std::move(t_ts2),          std::move(t_ts3),
          std::move(t_val), std::move(t_projection), std::move(t_updated_rows), std::move(t_update)};
}

inline std::vector<std::shared_ptr<OperatorTask>> commit() {
  auto commit = std::make_shared<CommitRecords>();
  return {std::make_shared<OperatorTask>(std::move(commit))};
}

inline void execute_tasks_with_context(std::vector<std::shared_ptr<OperatorTask>>& tasks,
                                       std::shared_ptr<TransactionContext> t_context) {
  for (auto job_itr = tasks.begin(), end = tasks.end(); job_itr != end; ++job_itr) {
    (*job_itr)->get_operator()->set_transaction_context(t_context);
    (*job_itr)->schedule();
  }
  tasks.back()->join();
}

BENCHMARK_F(TPCCBenchmarkFixture, BM_delivery)(benchmark::State& state) {
  clear_cache();

  // currently no warm up

  int w_id = _random_gen.number(0, _gen._warehouse_size - 1);
  int d_id = 0;
  while (state.KeepRunning()) {
    auto t_context = TransactionManager::get().new_transaction_context();
    d_id = (d_id + 1) % _gen._district_size;
    int o_carrier_id = _random_gen.number(1, 10);
    const time_t datetime = std::time(0);
    auto tasks1 = delivery_getDId(d_id, w_id);
    execute_tasks_with_context(tasks1, t_context);

    assert(tasks1.back()->get_operator()->get_output()->row_count() > 0);
    auto no_o_id = tasks1.back()->get_operator()->get_output()->get_value<int>(0u, 0u);
    auto tasks2 = delivery_del(no_o_id);
    execute_tasks_with_context(tasks2, t_context);

    auto tasks3 = delivery_selOrder(d_id, w_id, no_o_id);
    execute_tasks_with_context(tasks3, t_context);

    assert(tasks3.back()->get_operator()->get_output()->row_count() > 0);
    auto c_id = tasks3.back()->get_operator()->get_output()->get_value<int>(0u, 0u);

    auto tasks4 = delivery_updateOrder(d_id, w_id, no_o_id, o_carrier_id);
    execute_tasks_with_context(tasks4, t_context);

    auto tasks5 = delivery_updateOrderLine(d_id, w_id, no_o_id, datetime);
    execute_tasks_with_context(tasks5, t_context);

    auto tasks6 = delivery_sumOrderLine(d_id, w_id, no_o_id);
    execute_tasks_with_context(tasks6, t_context);

    assert(tasks6.back()->get_operator()->get_output()->row_count() > 0);
    auto ol_total = tasks6.back()->get_operator()->get_output()->get_value<double>(0u, 0u);
    auto tasks7 = delivery_updateCustomer(ol_total, d_id, w_id, c_id);
    execute_tasks_with_context(tasks7, t_context);

    // Commit transaction.
    TransactionManager::get().prepare_commit(*t_context);
    auto commit_task = commit();
    execute_tasks_with_context(commit_task, t_context);
    TransactionManager::get().commit(*t_context);
  }
}

}  // namespace opossum
