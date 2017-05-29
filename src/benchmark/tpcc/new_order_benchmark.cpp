#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../../lib/concurrency/transaction_manager.hpp"
#include "../../lib/operators/commit_records.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/product.hpp"
#include "../../lib/operators/projection.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/operators/update.hpp"
#include "../../lib/scheduler/operator_task.hpp"

#include "tpcc_base_fixture.cpp"

namespace opossum {

class TPCCNewOrderBenchmark : public TPCCBenchmarkFixture {
 public:
  std::vector<std::shared_ptr<OperatorTask>> get_get_customer_and_warehouse_tax_rate_tasks(
      const std::shared_ptr<TransactionContext> t_context, const int32_t w_id, const int32_t d_id, const int32_t c_id) {
    /**
     * SELECT c_discount, c_last, c_credit, w_tax
     * FROM customer, warehouse
     * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id
     */

    // Operators
    const auto c_gt = std::make_shared<GetTable>("CUSTOMER");
    const auto c_ts1 = std::make_shared<TableScan>(c_gt, "C_W_ID", "=", w_id);
    const auto c_ts2 = std::make_shared<TableScan>(c_ts1, "C_D_ID", "=", d_id);
    const auto c_ts3 = std::make_shared<TableScan>(c_ts2, "C_ID", "=", c_id);

    const auto w_gt = std::make_shared<GetTable>("WAREHOUSE");
    const auto w_ts = std::make_shared<TableScan>(w_gt, "W_ID", "=", w_id);

    // Both operators should have exactly one row -> Product operator should have smallest overhead.
    const auto join = std::make_shared<Product>(c_ts3, w_ts);

    const std::vector<std::string> columns = {"C_DISCOUNT", "C_LAST", "C_CREDIT", "W_TAX"};
    const auto proj = std::make_shared<Projection>(join, columns);

    set_transaction_context_for_operators(t_context, {c_gt, c_ts1, c_ts2, c_ts3, w_gt, w_ts, join, proj});

    // Tasks
    const auto c_gt_t = std::make_shared<OperatorTask>(c_gt);
    const auto c_ts1_t = std::make_shared<OperatorTask>(c_ts1);
    const auto c_ts2_t = std::make_shared<OperatorTask>(c_ts2);
    const auto c_ts3_t = std::make_shared<OperatorTask>(c_ts3);

    const auto w_gt_t = std::make_shared<OperatorTask>(w_gt);
    const auto w_ts_t = std::make_shared<OperatorTask>(w_ts);

    const auto join_t = std::make_shared<OperatorTask>(join);
    const auto proj_t = std::make_shared<OperatorTask>(proj);

    // Dependencies
    c_gt_t->set_as_predecessor_of(c_ts1_t);
    c_ts1_t->set_as_predecessor_of(c_ts2_t);
    c_ts2_t->set_as_predecessor_of(c_ts3_t);

    w_gt_t->set_as_predecessor_of(w_ts_t);

    c_ts3_t->set_as_predecessor_of(join_t);
    w_ts_t->set_as_predecessor_of(join_t);

    join_t->set_as_predecessor_of(proj_t);

    return {c_gt_t, c_ts1_t, c_ts2_t, c_ts3_t, w_gt_t, w_ts_t, join_t, proj_t};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
                                                                    const int32_t d_id, const int32_t w_id) {
    /**
     * SELECT d_next_o_id, d_tax
     * FROM district
     * WHERE d_id = :d_id AND d_w_id = :w_id
     */

    // Operators
    const auto gt = std::make_shared<GetTable>("DISTRICT");

    const auto ts1 = std::make_shared<TableScan>(gt, "D_ID", "=", d_id);
    const auto ts2 = std::make_shared<TableScan>(ts1, "D_W_ID", "=", w_id);

    const std::vector<std::string> columns = {"D_TAX", "D_NEXT_O_ID"};
    const auto proj = std::make_shared<Projection>(ts2, columns);

    set_transaction_context_for_operators(t_context, {gt, ts1, ts2, proj});

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
      const std::shared_ptr<TransactionContext> t_context, const int32_t d_id, const int32_t d_w_id,
      const int32_t d_next_o_id) {
    /**
     * UPDATE district
     * SET d_next_o_id = :d_next_o_id + 1
     * WHERE d_id = :d_id AND d_w_id = :w_id
     */

    // Operators
    const auto gt = std::make_shared<GetTable>("DISTRICT");
    const auto ts1 = std::make_shared<TableScan>(gt, "D_ID", "=", d_id);
    const auto ts2 = std::make_shared<TableScan>(ts1, "D_W_ID", "=", d_w_id);

    const std::vector<std::string> columns = {"D_NEXT_O_ID"};
    const auto original_rows = std::make_shared<Projection>(ts2, columns);

    const Projection::ProjectionDefinitions definitions{
        Projection::ProjectionDefinition{std::to_string(d_next_o_id) + "+1", "int", "fix"}};
    const auto updated_rows = std::make_shared<Projection>(ts2, definitions);

    const auto update = std::make_shared<Update>("DISTRICT", original_rows, updated_rows);

    set_transaction_context_for_operators(t_context, {gt, ts1, ts2, original_rows, updated_rows, update});

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

  // TODO(tim): re-factor/extend Insert so that we do not have to build a Table object first.
  //  std::vector<std::shared_ptr<OperatorTask>> get_create_order_tasks(
  //          const std::shared_ptr<TransactionContext> t_context, const int32_t d_next_o_id, const int32_t d_id, const
  //          int32_t w_id, const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id, const int32_t
  //          o_ol_cnt, const int32_t o_all_local) {
  //    /**
  //     * INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
  //     * VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  //     */
  //  }
  //
  //  std::vector<std::shared_ptr<OperatorTask>> get_create_new_order_tasks(
  //          const std::shared_ptr<TransactionContext> t_context, const int32_t o_id, const int32_t d_id, const int32_t
  //          w_id) {
  //    /**
  //     * INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)
  //     */
  //  }

  std::vector<std::shared_ptr<OperatorTask>> get_get_item_info_tasks(
      const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id) {
    /**
     * SELECT i_price, i_name , i_data
     * FROM item
     * WHERE i_id = :ol_i_id;
     */

    // Operators
    auto gt = std::make_shared<GetTable>("ITEM");
    auto ts = std::make_shared<TableScan>(gt, "I_ID", "=", ol_i_id);

    const std::vector<std::string> columns = {"I_PRICE", "I_NAME", "I_DATA"};
    auto proj = std::make_shared<Projection>(ts, columns);

    set_transaction_context_for_operators(t_context, {gt, ts, proj});

    // Tasks
    auto gt_t = std::make_shared<OperatorTask>(gt);
    auto ts_t = std::make_shared<OperatorTask>(ts);
    auto proj_t = std::make_shared<OperatorTask>(proj);

    // Dependencies
    gt_t->set_as_predecessor_of(ts_t);
    ts_t->set_as_predecessor_of(proj_t);

    return {gt_t, ts_t, proj_t};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_get_stock_info_tasks(
      const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id, const int32_t ol_supply_w_id) {
    /**
     * SELECT
     *  s_quantity, s_data,
     *  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05 s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
     * FROM stock
     * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
     */

    // Operators
    auto gt = std::make_shared<GetTable>("STOCK");
    auto ts1 = std::make_shared<TableScan>(gt, "S_I_ID", "=", ol_i_id);
    auto ts2 = std::make_shared<TableScan>(ts1, "S_W_ID", "=", ol_supply_w_id);

    std::vector<std::string> columns = {"S_DIST_01", "S_DIST_02", "S_DIST_03", "S_DIST_04", "S_DIST_05",
                                        "S_DIST_06", "S_DIST_07", "S_DIST_08", "S_DIST_09", "S_DIST_10"};
    auto proj = std::make_shared<Projection>(ts2, columns);

    set_transaction_context_for_operators(t_context, {gt, ts1, ts2, proj});

    // Tasks
    auto gt_t = std::make_shared<OperatorTask>(gt);
    auto ts1_t = std::make_shared<OperatorTask>(ts1);
    auto ts2_t = std::make_shared<OperatorTask>(ts2);
    auto proj_t = std::make_shared<OperatorTask>(proj);

    // Dependencies
    gt_t->set_as_predecessor_of(ts1_t);
    ts1_t->set_as_predecessor_of(ts2_t);
    ts2_t->set_as_predecessor_of(proj_t);

    return {gt_t, ts1_t, ts2_t, proj_t};
  }

  std::vector<std::shared_ptr<OperatorTask>> get_update_stock_tasks(const std::shared_ptr<TransactionContext> t_context,
                                                                    const int32_t s_quantity, const int32_t ol_i_id,
                                                                    const int32_t ol_supply_w_id) {
    /**
     * UPDATE stock
     * SET s_quantity = :s_quantity
     * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id
     */

    // Operators
    const auto gt = std::make_shared<GetTable>("STOCK");
    const auto ts1 = std::make_shared<TableScan>(gt, "S_I_ID", "=", ol_i_id);
    const auto ts2 = std::make_shared<TableScan>(ts1, "S_W_ID", "=", ol_supply_w_id);

    const std::vector<std::string> columns = {"S_QUANTITY"};
    const auto original_rows = std::make_shared<Projection>(ts2, columns);

    const Projection::ProjectionDefinitions definitions{
        Projection::ProjectionDefinition{std::to_string(s_quantity), "int", "fix"}};
    const auto updated_rows = std::make_shared<Projection>(ts2, definitions);

    const auto update = std::make_shared<Update>("STOCK", original_rows, updated_rows);

    set_transaction_context_for_operators(t_context, {gt, ts1, ts2, original_rows, updated_rows, update});

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
  auto w_id = 1;
  auto d_next_o_id = 1;  // TODO(tim): read result from previous SELECT?
  auto c_id = 1;
  //  auto o_entry_d = 1;
  //  auto o_carrier_id = 1;
  //  auto o_ol_cnt = 1;
  //  auto o_all_local = 1;
  auto ol_i_id = 1;
  auto ol_supply_w_id = 1;
  auto s_quantity = 1;

  while (state.KeepRunning()) {
    auto t_context = TransactionManager::get().new_transaction_context();

    auto get_customer_tasks = get_get_customer_and_warehouse_tax_rate_tasks(t_context, w_id, d_id, c_id);
    AbstractScheduler::schedule_tasks_and_wait(get_customer_tasks);

    auto get_district_tasks = get_get_district_tasks(t_context, d_id, w_id);
    AbstractScheduler::schedule_tasks_and_wait(get_district_tasks);

    //    auto output = last_get_district_task->get_operator()->get_output();

    auto increment_next_order_id_tasks = get_increment_next_order_id_tasks(t_context, d_id, w_id, d_next_o_id);
    AbstractScheduler::schedule_tasks_and_wait(increment_next_order_id_tasks);

    // TODO(tim): loop
    auto get_item_info_tasks = get_get_item_info_tasks(t_context, ol_i_id);
    AbstractScheduler::schedule_tasks_and_wait(get_item_info_tasks);

    auto get_stock_info_tasks = get_get_stock_info_tasks(t_context, ol_i_id, ol_supply_w_id);
    AbstractScheduler::schedule_tasks_and_wait(get_stock_info_tasks);

    auto update_stock_tasks = get_update_stock_tasks(t_context, s_quantity, ol_i_id, ol_supply_w_id);
    AbstractScheduler::schedule_tasks_and_wait(update_stock_tasks);

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
