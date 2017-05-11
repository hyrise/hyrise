#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/delete.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/scheduler/abstract_task.hpp"
#include "../../lib/scheduler/operator_task.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "tpcc_base_fixture.cpp"

namespace opossum {

inline std::vector<std::shared_ptr<OperatorTask>> getNewOrder_getDId(
    std::shared_ptr<TransactionContext> transaction_context, const int d_id, const int w_id) {
  // SELECT NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1

  auto new_order = std::make_shared<GetTable>("NEW-ORDER");
  // new_order->set_transaction_context(transaction_context);
  auto ts1 = std::make_shared<TableScan>(new_order, ColumnName("NO_D_ID"), "=", d_id);
  // ts1->set_transaction_context(transaction_context);
  auto ts2 = std::make_shared<TableScan>(ts1, ColumnName("NO_W_ID"), "=", w_id);
  // ts2->set_transaction_context(transaction_context);
  // auto limit = std::make_shared<TableScan>(ts3, 1);
  auto ts3 = std::make_shared<TableScan>(ts2, ColumnName("NO_O_ID"), ">", -1);
  // ts3->set_transaction_context(transaction_context);
  Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$NO_O_ID", "float", "NO_O_ID"}};
  auto proj = std::make_shared<Projection>(ts3, std::move(definitions));
  // proj->set_transaction_context(transaction_context);

  auto j_new_order = std::make_shared<OperatorTask>(std::move(new_order));
  auto j_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  auto j_ts2 = std::make_shared<OperatorTask>(std::move(ts2));
  auto j_ts3 = std::make_shared<OperatorTask>(std::move(ts3));
  auto j_proj = std::make_shared<OperatorTask>(std::move(proj));

  j_new_order->set_as_predecessor_of(j_ts1);
  j_ts1->set_as_predecessor_of(j_ts2);
  j_ts2->set_as_predecessor_of(j_ts3);
  j_ts3->set_as_predecessor_of(j_proj);

  return {j_new_order, j_ts1, j_ts2, j_ts3, j_proj};
}

inline std::vector<std::shared_ptr<OperatorTask>> getNewOrder_del(
    std::shared_ptr<TransactionContext> transaction_context, const int no_o_id) {
  auto new_order = std::make_shared<GetTable>("NEW-ORDER");
  // new_order->set_transaction_context(transaction_context);
  auto ts1 = std::make_shared<TableScan>(new_order, ColumnName("NO_O_ID"), "=", no_o_id);
  // ts1->set_transaction_context(transaction_context);
  // auto delete_op = std::make_shared<Delete>("NEW-ORDER", ts1);
  // delete_op->set_transaction_context(transaction_context);

  auto j_new_order = std::make_shared<OperatorTask>(std::move(new_order));
  auto j_ts1 = std::make_shared<OperatorTask>(std::move(ts1));
  // auto j_delete_op = std::make_shared<OperatorTask>(std::move(delete_op));

  j_new_order->set_as_predecessor_of(j_ts1);
  // j_ts1->set_as_predecessor_of(j_delete_op);

  return {j_new_order, j_ts1};
}

BENCHMARK_F(TPCCBenchmarkFixture, BM_getNetOrder)(benchmark::State& state) {
  clear_cache();
  // Projection::ProjectionDefinitions definitions{Projection::ProjectionDefinition{"$NO_O_ID", "float", "NO_O_ID"}};
  // auto warm_up = std::make_shared<TableScan>(_gt_new_order, ColumnName("NO_O_ID"), ">", -1);
  // warm_up->execute();
  // auto t_context = TransactionManager::get().new_transaction_context();
  // auto _gt_item = std::make_shared<GetTable>("ITEM");
  // auto _gt_warehouse = std::make_shared<GetTable>("WAREHOUSE");
  // auto _gt_stock = std::make_shared<GetTable>("STOCK");
  // auto _gt_district = std::make_shared<GetTable>("DISTRICT");
  // auto _gt_customer = std::make_shared<GetTable>("CUSTOMER");
  // auto _gt_order = std::make_shared<GetTable>("ORDER");
  // auto _gt_order_line = std::make_shared<GetTable>("ORDER");
  // _gt_item->execute();
  // _gt_warehouse->execute();
  // _gt_stock->execute();
  // _gt_district->execute();
  // _gt_customer->execute();
  // _gt_order->execute();
  // _gt_order_line->execute();

  while (state.KeepRunning()) {
    for (size_t w_id = 0; w_id < _gen._warehouse_size; ++w_id) {
      for (size_t d_id = 0; d_id < _gen._district_size; ++d_id) {
        auto transaction_context = TransactionManager::get().new_transaction_context();
        auto jobs = getNewOrder_getDId(transaction_context, w_id, d_id);
        std::vector<std::shared_ptr<AbstractTask>> a_jobs;
        for (auto job_itr = jobs.begin(), end = jobs.end(); job_itr != end; ++job_itr) {
          (*job_itr)->schedule();
          a_jobs.emplace_back(*job_itr);
        }
        // CurrentScheduler::wait_for_tasks(a_jobs);
        // int no_o_id = 1;  // jobs.back()->get_operator()->get_output()->get_chunk(0u).get_column(0u)->at(0u);

        // jobs = getNewOrder_del(transaction_context, no_o_id);
        // a_jobs.clear();
        // for (auto job_itr = jobs.begin(), end = jobs.end(); job_itr != end; ++job_itr) {
        //   (*job_itr)->schedule();
        //   a_jobs.emplace_back(*job_itr);
        // }
        // CurrentScheduler::wait_for_tasks(a_jobs);

        // auto ts5 = std::make_shared<TableScan>(_gt_order, ColumnName("NO_D_ID"), "=", d_id);
        // ts1->execute();
        // auto ts6 = std::make_shared<TableScan>(ts1, ColumnName("NO_W_ID"), "=", w_id);
        // ts2->execute();
        // auto ts7 = std::make_shared<TableScan>(ts2, ColumnName("NO_O_ID"), ">", -1);
        // ts3->execute();
      }
    }
  }
}

}  // namespace opossum
