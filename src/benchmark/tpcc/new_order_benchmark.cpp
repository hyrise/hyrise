#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/commit_records.hpp"
#include "../../lib/operators/get_table.hpp"
//#include "../../lib/operators/print.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {
 public:
  //  TODO(tim): clarify query since it is different in A. Pavlo's repo.
  //  std::vector<std::shared_ptr<OperatorTask>> get_warehouse_tax_rate_tasks() {
  //  }

  std::vector<std::shared_ptr<OperatorTask>> get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
                                                                    const int d_id, const int d_w_id) {
    /**
     * SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?
     */

    // Operators
    const auto gt = std::make_shared<GetTable>("DISTRICT");

    const auto ts1 = std::make_shared<TableScan>(gt, "D_ID", "=", d_id);
    const auto ts2 = std::make_shared<TableScan>(ts1, "D_W_ID", "=", d_w_id);

    const std::vector<std::string> columns = {"D_TAX", "D_NEXT_O_ID"};
    const auto proj = std::make_shared<Projection>(ts2, columns);

    const std::vector<std::shared_ptr<AbstractOperator>> operators = {gt, ts1, ts2, proj};
    set_transaction_context_for_operators(t_context, operators);

    // Tasks
    const auto gt_t = std::make_shared<OperatorTask>(gt);
    const auto ts1_t = std::make_shared<OperatorTask>(ts1);
    const auto ts2_t = std::make_shared<OperatorTask>(ts2);
    const auto proj_t = std::make_shared<OperatorTask>(proj);

    // Dependencies
    gt_t->set_as_predecessor_of(ts1_t);
    ts1_t->set_as_predecessor_of(ts2_t);
    ts2_t->set_as_predecessor_of(proj_t);

    return {gt_t, ts1_t, ts2_t, proj_t};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_increment_next_order_id_tasks(
      const std::shared_ptr<TransactionContext> t_context, const int d_id, const int d_w_id, const int d_next_o_id) {
    /**
     * UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?
     */

    // Operators
    const auto gt = std::make_shared<GetTable>("DISTRICT");
    const auto ts1 = std::make_shared<TableScan>(gt, "D_ID", "=", d_id);
    const auto ts2 = std::make_shared<TableScan>(ts1, "D_W_ID", "=", d_w_id);

    const std::vector<std::string> columns = {"D_NEXT_O_ID"};
    const auto original_rows = std::make_shared<Projection>(ts2, columns);

    const Projection::ProjectionDefinitions definitions{
        Projection::ProjectionDefinition{std::to_string(d_next_o_id), "int", "fix"}};
    const auto updated_rows = std::make_shared<Projection>(ts2, definitions);

    const auto update = std::make_shared<Update>("DISTRICT", original_rows, updated_rows);

    const std::vector<std::shared_ptr<AbstractOperator>> operators = {gt,           ts1,   ts2, original_rows,
                                                                      updated_rows, update};
    set_transaction_context_for_operators(t_context, operators);

    // Tasks
    const auto gt_t = std::make_shared<OperatorTask>(gt);
    const auto ts1_t = std::make_shared<OperatorTask>(ts1);
    const auto ts2_t = std::make_shared<OperatorTask>(ts2);
    const auto original_rows_t = std::make_shared<OperatorTask>(original_rows);
    const auto updated_rows_t = std::make_shared<OperatorTask>(updated_rows);
    const auto update_t = std::make_shared<OperatorTask>(update);

    // Dependencies
    gt_t->set_as_predecessor_of(ts1_t);
    ts1_t->set_as_predecessor_of(ts2_t);

    ts2_t->set_as_predecessor_of(original_rows_t);
    ts2_t->set_as_predecessor_of(updated_rows_t);

    original_rows_t->set_as_predecessor_of(update_t);
    updated_rows_t->set_as_predecessor_of(update_t);

    return {gt_t, ts1_t, ts2_t, original_rows_t, updated_rows_t, update_t};
  }
};

BENCHMARK_F(TPCCNewOrderBenchmark, BM_TPCC_NewOrder)(benchmark::State& state) {
  clear_cache();

  auto d_id = 1;
  auto d_w_id = 1;
  auto d_next_o_id = 1;

  while (state.KeepRunning()) {
    auto t_context = TransactionManager::get().new_transaction_context();

    auto get_district_tasks = get_get_district_tasks(t_context, d_id, d_w_id);
    schedule_tasks(get_district_tasks);

    auto last_get_district_task = get_district_tasks.back();
    last_get_district_task->join();

    //    auto output = last_get_district_task->get_operator()->get_output();

    auto increment_next_order_id_tasks = get_increment_next_order_id_tasks(t_context, d_id, d_w_id, d_next_o_id);
    schedule_tasks(increment_next_order_id_tasks);

    auto last_increment_next_order_id_task = increment_next_order_id_tasks.back();
    last_increment_next_order_id_task->join();

    // Commit transaction.
    TransactionManager::get().prepare_commit(*t_context);

    auto commit = std::make_shared<CommitRecords>();
    commit->set_transaction_context(t_context);

    auto commit_task = std::make_shared<OperatorTask>(commit);
    commit_task->schedule();
    commit_task->join();

    TransactionManager::get().commit(*t_context);
  }
}

}  // namespace opossum
