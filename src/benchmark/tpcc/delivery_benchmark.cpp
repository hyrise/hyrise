#include <cassert>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/aggregate.hpp"
#include "../../lib/operators/delete.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/scheduler/operator_task.hpp"

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
  //  // auto limit = std::make_shared<TableScan>(ts3, 1);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("NO_O_ID"), ">", -1);
  auto proj = std::make_shared<Projection>(ts3, std::vector<std::string>{"NO_O_ID"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_proj = std::make_shared<OperatorTask>(std::move(proj));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_proj);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_proj)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_del(const int no_o_id) {
  /**
   * EXEC SQL DELETE
   * FROM new_order
   * WHERE CURRENT OF c_no;
   */
  auto gt = std::make_shared<GetTable>("NEW-ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("NO_O_ID"), "=", no_o_id);
  // auto delete_op = std::make_shared<Delete>("NEW-ORDER", ts1);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  // auto t_delete_op = std::make_shared<OperatorTask>(std::move(delete_op));

  t_gt->set_as_predecessor_of(t_ts1);
  // t_ts1->set_as_predecessor_of(t_delete_op);

  return {std::move(t_gt), std::move(t_ts1)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_selOrder(const int d_id, const int w_id,
                                                                       const int no_o_id) {
  /**
   * EXEC SQL SELECT o_c_id INTO :c_id
   * FROM orders
   * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("O_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("O_W_ID"), "=", w_id);
  auto proj = std::make_shared<Projection>(ts3, std::vector<std::string>{"O_C_ID"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_proj = std::make_shared<OperatorTask>(std::move(proj));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_proj);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_proj)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateOrder(const int d_id, const int w_id,
                                                                          const int no_o_id) {
  /**
   * EXEC SQL UPDATE orders
   * SET o_carrier_id = :o_carrier_id
   * WHERE o_id = :no_o_id AND o_d_id = :d_id AND o_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("O_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("O_W_ID"), "=", w_id);
  auto proj = std::make_shared<Projection>(ts3, std::vector<std::string>{"O_CARRIER_ID"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_proj = std::make_shared<OperatorTask>(std::move(proj));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_proj);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_proj)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateOrderLine(const int d_id, const int w_id,
                                                                              const int no_o_id) {
  /**
   * EXEC SQL UPDATE order_line
   * SET ol_delivery_d = :datetime
   * WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND ol_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("ORDER-LINE");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("OL_O_ID"), "=", no_o_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("OL_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("OL_W_ID"), "=", w_id);
  auto proj = std::make_shared<Projection>(ts3, std::vector<std::string>{"OL_DELIVERY_D"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_proj = std::make_shared<OperatorTask>(std::move(proj));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_proj);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_proj)};
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
//  auto sum = std::make_shared<Aggregate>(
//      ts3, std::vector<std::pair<std::string, AggregateFunction>>{std::make_pair(std::string("OL_AMOUNT"), Sum)},
//      std::vector<std::string>{});
  auto sum = std::make_shared<Projection>(ts3, std::vector<std::string>{"OL_AMOUNT"});

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_sum = std::make_shared<OperatorTask>(std::move(sum));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_sum);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_sum)};
}

inline std::vector<std::shared_ptr<OperatorTask>> delivery_updateCustomer(
    Projection::ProjectionDefinitions& definitions, const int d_id, const int w_id, const int c_id) {
  /**
   * EXEC SQL UPDATE customer
   * SET c_balance = c_balance + :ol_total
   * WHERE c_id = :c_id AND c_d_id = :d_id AND c_w_id = :w_id;
   */
  auto gt = std::make_shared<GetTable>("CUSTOMER");
  auto ts1 = std::make_shared<TableScan>(gt, ColumnName("C_ID"), "=", c_id);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("C_D_ID"), "=", d_id);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("C_W_ID"), "=", w_id);
  auto proj = std::make_shared<Projection>(ts3, definitions);

  auto t_gt = std::make_shared<OperatorTask>(std::move(gt));
  auto t_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto t_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto t_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto t_proj = std::make_shared<OperatorTask>(std::move(proj));

  t_gt->set_as_predecessor_of(t_ts1);
  t_ts1->set_as_predecessor_of(t_ts2);
  t_ts2->set_as_predecessor_of(t_ts3);
  t_ts3->set_as_predecessor_of(t_proj);

  return {std::move(t_gt), std::move(t_ts1), std::move(t_ts2), std::move(t_ts3), std::move(t_proj)};
}

inline void schedule_tasks(std::vector<std::shared_ptr<OperatorTask>> tasks) {
  for (auto job_itr = tasks.begin(), end = tasks.end(); job_itr != end; ++job_itr) {
    (*job_itr)->schedule();
  }
}

inline void join_tasks(std::vector<std::shared_ptr<OperatorTask>> tasks) {
  for (auto job_itr = tasks.begin(), end = tasks.end(); job_itr != end; ++job_itr) {
    (*job_itr)->join();
  }
}

inline void execute_tasks(std::vector<std::shared_ptr<OperatorTask>> tasks) {
  schedule_tasks(tasks);
  join_tasks(std::move(tasks));
}

template <typename T>
inline T get_value_from_table(std::shared_ptr<OperatorTask> task, size_t row_id = 0u, size_t column_id = 0u) {
  size_t previous_rows = 0;
  for (ChunkID chunk_id = 0, chunk_count = task->get_operator()->get_output()->chunk_count(); chunk_id < chunk_count;
       ++chunk_id) {
    size_t current_size = task->get_operator()->get_output()->get_chunk(chunk_id).size();
    if (previous_rows + current_size > row_id) {
      return get<T>(
          (*task->get_operator()->get_output()->get_chunk(chunk_id).get_column(column_id))[row_id - previous_rows]);
    }
    previous_rows += current_size;
  }
  throw std::logic_error("row_id out of bounds");
}

BENCHMARK_F(TPCCBenchmarkFixture, BM_getNetOrder)(benchmark::State& state) {
  clear_cache();

  // currently no warm up

  while (state.KeepRunning()) {
    int w_id = _random_gen.number(0, _gen._warehouse_size - 1);
    int d_id = _random_gen.number(0, _gen._district_size - 1);
    auto tasks = delivery_getDId(d_id, w_id);
    execute_tasks(tasks);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto no_o_id = get_value_from_table<int>(tasks.back());
    tasks = delivery_del(no_o_id);
    execute_tasks(tasks);

    tasks = delivery_selOrder(d_id, w_id, no_o_id);
    execute_tasks(tasks);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto c_id = get_value_from_table<int>(tasks.back());

    tasks = delivery_updateOrder(d_id, w_id, no_o_id);
    execute_tasks(tasks);

    tasks = delivery_updateOrderLine(d_id, w_id, no_o_id);
    execute_tasks(tasks);

    tasks = delivery_sumOrderLine(d_id, w_id, no_o_id);
    execute_tasks(tasks);

    assert(tasks.back()->get_operator()->get_output()->row_count() > 0);
    auto ol_total = get_value_from_table<float>(tasks.back());
    Projection::ProjectionDefinitions definitions{
        Projection::ProjectionDefinition{"$C_BALANCE+" + std::to_string(ol_total), "float", "C_BALANCE"}};
    tasks = delivery_updateCustomer(definitions, d_id, w_id, c_id);
    execute_tasks(tasks);
  }
}

}  // namespace opossum
