#include "new_order.hpp"

#include <boost/variant.hpp>

#include "concurrency/commit_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/commit_records.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/table_scan.hpp"
#include "operators/product.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/projection.hpp"
#include "scheduler/abstract_scheduler.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/storage_manager.hpp"
#include "utils/helper.hpp"
#include "all_type_variant.hpp"

using namespace opossum;

namespace tpcc {

NewOrderResult AbstractNewOrderImpl::run_transaction(const NewOrderParams &params) {
  NewOrderResult result;
  std::vector<AllTypeVariant> row;

  auto t_context = TransactionManager::get().new_transaction_context();

  /**
   * GET CUSTOMER AND WAREHOUSE TAX RATE
   */
  auto get_customer_and_warehouse_tax_rate_tasks = get_get_customer_and_warehouse_tax_rate_tasks(t_context, params.w_id,
                                                                                                 params.d_id,
                                                                                                 params.c_id);
  AbstractScheduler::schedule_tasks_and_wait(get_customer_and_warehouse_tax_rate_tasks);

  const auto customer_and_warehouse_tax_rate_table = get_customer_and_warehouse_tax_rate_tasks.back()->get_operator()->
    get_output();

  result.c_discount = customer_and_warehouse_tax_rate_table->get_value<float>(0, 0);
  result.c_last = customer_and_warehouse_tax_rate_table->get_value<std::string>(1, 0);
  result.c_credit = customer_and_warehouse_tax_rate_table->get_value<std::string>(2, 0);
  result.w_tax_rate = customer_and_warehouse_tax_rate_table->get_value<float>(3, 0);

  /**
   * GET DISTRICT
   */
  auto get_district_tasks = get_get_district_tasks(t_context, params.d_id, params.w_id);
  AbstractScheduler::schedule_tasks_and_wait(get_district_tasks);
  const auto districts_table = get_district_tasks.back()->get_operator()->get_output();

  result.d_next_o_id = districts_table->get_value<int32_t>(0, 0);
  result.d_tax_rate = districts_table->get_value<float>(1, 0);

  /**
   * INCREMENT NEXT ORDER ID
   */
  auto increment_next_order_id_tasks = get_increment_next_order_id_tasks(t_context, params.d_id, params.w_id,
                                                                         result.d_next_o_id);
  AbstractScheduler::schedule_tasks_and_wait(increment_next_order_id_tasks);

  /**
   * CREATE ORDER
   */
  auto create_order_tasks = get_create_order_tasks(t_context, result.d_next_o_id, params.d_id, params.w_id,
    params.c_id, params.o_entry_d, 0, params.order_lines.size(), 1);
  AbstractScheduler::schedule_tasks_and_wait(create_order_tasks);

  /**
   * CREATE NEW ORDER
   */
  auto create_new_order_tasks = get_create_new_order_tasks(t_context, result.d_next_o_id, params.d_id, params.w_id);
  AbstractScheduler::schedule_tasks_and_wait(create_new_order_tasks);

//  std::vector<std::shared_ptr<OperatorTask>> all_tasks;
//
//  for (size_t ol_idx = 0; ol_idx < params.order_lines.size(); ol_idx++) {
//    const auto &order_line_params = params.order_lines[ol_idx];
//
//    NewOrderOrderLineResult order_line;
//
//    /**
//     * GET ITEM INFO
//     */
//    auto get_item_info_tasks = get_get_item_info_tasks(t_context, order_line_params.i_id);
//    AbstractScheduler::schedule_tasks_and_wait(get_item_info_tasks);
//    const auto item_info_table = get_item_info_tasks.back()->get_operator()->get_output();
//
//    order_line.i_price = item_info_table->get_value<float>(0, 0);
//    order_line.i_name = item_info_table->get_value<std::string>(1, 0);
//    order_line.i_data = item_info_table->get_value<std::string>(2, 0);
//
//    /**
//     * GET STOCK INFO
//     */
//    auto get_stock_info_tasks = get_get_stock_info_tasks(t_context, order_line_params.i_id, order_line_params.w_id,
//                                                         params.d_id);
//    AbstractScheduler::schedule_tasks_and_wait(get_stock_info_tasks);
//    const auto stock_info_table = get_stock_info_tasks.back()->get_operator()->get_output();
//
//    order_line.s_qty = stock_info_table->get_value<int32_t>(0, 0);
//    order_line.s_data = stock_info_table->get_value<std::string>(1, 0);
//    order_line.s_ytd = stock_info_table->get_value<int32_t>(2, 0);
//    order_line.s_order_cnt = stock_info_table->get_value<int32_t>(3, 0);
//    order_line.s_remote_cnt = stock_info_table->get_value<int32_t>(4, 0);
//    order_line.s_dist_xx = stock_info_table->get_value<std::string>(5, 0);
//
//    /**
//     * Calculate new s_ytd, s_qty and s_order_cnt
//     */
//    //auto s_ytd = order_line.s_ytd + order_line_params.qty; // TODO: why doesn't the tpc ref impl update this in UPDATE STOCK?
//
//    int32_t s_qty = order_line.s_qty;
//    if (order_line.s_qty >= order_line_params.qty + 10) {
//      s_qty -= order_line_params.qty;
//    } else {
//      s_qty += 91 - order_line_params.qty;
//    }
//
//    //auto s_order_cnt = order_line.s_order_cnt + 1; // TODO: why doesn't the tpc ref impl update this in UPDATE STOCK?
//    order_line.amount = order_line_params.qty * order_line.i_price;
//
//    /**
//     * UPDATE STOCK
//     */
//    auto update_stock_tasks = get_update_stock_tasks(t_context, s_qty, order_line_params.i_id,
//                                                     order_line_params.w_id);
//    AbstractScheduler::schedule_tasks_and_wait(update_stock_tasks);
//
//    /**
//     * TODO(TIM) CREATE ORDER LINE
//     */
//    auto create_order_line_tasks = get_create_order_line_tasks(t_context,
//                                                               result.d_next_o_id,
//                                                               params.d_id,
//                                                               params.w_id,
//                                                               ol_idx + 1,
//                                                               order_line_params.i_id,
//                                                               0, // ol_supply_w_id - we only have one warehouse
//                                                               params.o_entry_d,
//                                                               order_line_params.qty,
//                                                               order_line.amount,
//                                                               order_line.s_dist_xx);
//    AbstractScheduler::schedule_tasks_and_wait(create_order_line_tasks);
//
//    std::copy(create_order_line_tasks.begin(), create_order_line_tasks.end(), std::back_inserter(all_tasks));
//
//    /**
//     * Add results
//     */
//    result.order_lines.emplace_back(order_line);
//  }

  /**
   * Commit
   */
  TransactionManager::get().prepare_commit(*t_context);

  auto commit = std::make_shared<CommitRecords>();
  commit->set_transaction_context(t_context);

  auto commit_task = std::make_shared<OperatorTask>(commit);
  commit_task->schedule();
  commit_task->join();

  TransactionManager::get().commit(*t_context);

  return result;
}

TaskVector NewOrderRefImpl::get_get_customer_and_warehouse_tax_rate_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t w_id, const int32_t d_id,
  const int32_t c_id) {
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

TaskVector
NewOrderRefImpl::get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
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

  const std::vector<std::string> columns = {"D_NEXT_O_ID", "D_TAX"};
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

TaskVector NewOrderRefImpl::get_increment_next_order_id_tasks(
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
TaskVector NewOrderRefImpl::get_create_order_tasks(const std::shared_ptr<TransactionContext> t_context,
                                                                  const int32_t d_next_o_id, const int32_t d_id,
                                                                  const int32_t w_id, const int32_t c_id,
                                                                  const int32_t o_entry_d, const int32_t o_carrier_id,
                                                                  const int32_t o_ol_cnt, const int32_t o_all_local) {
  /**
   * INSERT INTO ORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
   * VALUES (?, ?, ?, ?, ?, ?, ?, ?)
   */

  auto target_table_name = std::string("ORDER");
  const auto original_table = StorageManager::get().get_table(target_table_name);

  auto new_table = std::make_shared<Table>();
  for (ColumnID columnID = 0; columnID < original_table->col_count(); columnID++) {
    new_table->add_column(original_table->column_name(columnID), original_table->column_type(columnID), false);
  }

  Chunk chunk;
  chunk.add_column(create_single_value_column<int32_t>(d_next_o_id));
  chunk.add_column(create_single_value_column<int32_t>(d_id));
  chunk.add_column(create_single_value_column<int32_t>(w_id));
  chunk.add_column(create_single_value_column<int32_t>(c_id));
  chunk.add_column(create_single_value_column<int32_t>(o_entry_d));
  chunk.add_column(create_single_value_column<int32_t>(o_carrier_id));
  chunk.add_column(create_single_value_column<int32_t>(o_ol_cnt));
  chunk.add_column(create_single_value_column<int32_t>(o_all_local));
  new_table->add_chunk(std::move(chunk));

  auto tw = std::make_shared<TableWrapper>(new_table);
  const auto insert = std::make_shared<Insert>(target_table_name, tw);

  set_transaction_context_for_operators(t_context, {insert});

  const auto tw_t = std::make_shared<OperatorTask>(tw);
  const auto insert_t = std::make_shared<OperatorTask>(insert);

  return {tw_t, insert_t};
}

TaskVector NewOrderRefImpl::get_create_new_order_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t o_id, const int32_t d_id, const int32_t w_id) {
  /**
   * INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
   * VALUES (?, ?, ?);
   */

  auto target_table_name = std::string("NEW-ORDER");
  const auto original_table = StorageManager::get().get_table(target_table_name);

  auto new_table = std::make_shared<Table>();
  for (ColumnID columnID = 0; columnID < original_table->col_count(); columnID++) {
    new_table->add_column(original_table->column_name(columnID), original_table->column_type(columnID), false);
  }

  Chunk chunk;
  chunk.add_column(create_single_value_column<int32_t>(o_id));
  chunk.add_column(create_single_value_column<int32_t>(d_id));
  chunk.add_column(create_single_value_column<int32_t>(w_id));
  new_table->add_chunk(std::move(chunk));

  auto tw = std::make_shared<TableWrapper>(new_table);
  const auto insert = std::make_shared<Insert>(target_table_name, tw);

  set_transaction_context_for_operators(t_context, {tw, insert});

  const auto tw_t = std::make_shared<OperatorTask>(tw);
  const auto insert_t = std::make_shared<OperatorTask>(insert);

  return {tw_t, insert_t};
}

TaskVector NewOrderRefImpl::get_get_item_info_tasks(
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

TaskVector NewOrderRefImpl::get_get_stock_info_tasks(
  const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id, const int32_t ol_supply_w_id,
  const int32_t d_id) {
  /**
      * SELECT
      *  s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt
      *  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05 s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
      * FROM stock
      * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
      */

  // Operators
  auto gt = std::make_shared<GetTable>("STOCK");
  auto ts1 = std::make_shared<TableScan>(gt, "S_I_ID", "=", ol_i_id);
  auto ts2 = std::make_shared<TableScan>(ts1, "S_W_ID", "=", ol_supply_w_id);

  std::string s_dist_xx = d_id < 10 ? "S_DIST_0" + std::to_string(d_id) : "S_DIST_" + std::to_string(d_id);

  std::vector<std::string> columns = {"S_QUANTITY", "S_DATA", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT", s_dist_xx};
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

TaskVector
NewOrderRefImpl::get_update_stock_tasks(const std::shared_ptr<TransactionContext> t_context,
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

TaskVector NewOrderRefImpl::get_create_order_line_tasks(
  const std::shared_ptr<TransactionContext> t_context,
  const int32_t ol_o_id,
  const int32_t ol_d_id,
  const int32_t ol_w_id,
  const int32_t ol_number,
  const int32_t ol_i_id,
  const int32_t ol_supply_w_id,
  const int32_t ol_delivery_d,
  const int32_t ol_quantity,
  const float ol_amount,
  const std::string &ol_dist_info) {
  /**
   *  INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,
   *  ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
   *  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
   */

  auto target_table_name = std::string("ORDER-LINE");
  const auto original_table = StorageManager::get().get_table(target_table_name);

  auto new_table = std::make_shared<Table>();
  for (ColumnID columnID = 0; columnID < original_table->col_count(); columnID++) {
    new_table->add_column(original_table->column_name(columnID), original_table->column_type(columnID), false);
  }

  Chunk chunk;
  chunk.add_column(create_single_value_column<int32_t>(ol_o_id));
  chunk.add_column(create_single_value_column<int32_t>(ol_d_id));
  chunk.add_column(create_single_value_column<int32_t>(ol_w_id));
  chunk.add_column(create_single_value_column<int32_t>(ol_number));
  chunk.add_column(create_single_value_column<int32_t>(ol_i_id));
  chunk.add_column(create_single_value_column<int32_t>(ol_supply_w_id));
  chunk.add_column(create_single_value_column<int32_t>(ol_delivery_d));
  chunk.add_column(create_single_value_column<int32_t>(ol_quantity));
  chunk.add_column(create_single_value_column<float>(ol_amount));
  chunk.add_column(create_single_value_column<std::string>(ol_dist_info));
  new_table->add_chunk(std::move(chunk));

  auto tw = std::make_shared<TableWrapper>(new_table);
  const auto insert = std::make_shared<Insert>(target_table_name, tw);

  set_transaction_context_for_operators(t_context, {tw, insert});

  const auto tw_t = std::make_shared<OperatorTask>(tw);
  const auto insert_t = std::make_shared<OperatorTask>(insert);

  return {tw_t, insert_t};
}

}

namespace nlohmann {

using namespace opossum;
using namespace tpcc;

void adl_serializer<NewOrderParams>::to_json(nlohmann::json &j, const NewOrderParams &v) {
  throw "Not implemented";
}

void adl_serializer<NewOrderParams>::from_json(const nlohmann::json &j, NewOrderParams &v) {
  v.w_id = j["w_id"];
  v.d_id = j["d_id"];
  v.c_id = j["c_id"];
  v.o_entry_d = j["o_entry_d"];

  v.order_lines.reserve(j["order_lines"].size());
  for (const auto & ol_j : j["order_lines"]) {
    NewOrderOrderLineParams order_line_params = ol_j;
    v.order_lines.emplace_back(order_line_params);
  }
}

void adl_serializer<NewOrderOrderLineParams>::to_json(nlohmann::json &j, const NewOrderOrderLineParams &v) {
  throw "Not implemented";
}

void adl_serializer<NewOrderOrderLineParams>::from_json(const nlohmann::json &j, NewOrderOrderLineParams &v) {
  v.i_id = j["i_id"];
  v.w_id = j["w_id"];
  v.qty = j["qty"];
}

void adl_serializer<NewOrderOrderLineResult>::to_json(nlohmann::json &j, const NewOrderOrderLineResult &v) {
  throw "Not implemented";
}

void adl_serializer<NewOrderOrderLineResult>::from_json(const nlohmann::json &j, NewOrderOrderLineResult &v) {
  v.i_price = j["i_price"];
  v.i_name = j["i_name"];
  v.i_data = j["i_data"];
  v.s_qty = j["s_qty"];
  v.s_dist_xx = j["s_dist_xx"];
//  v.s_ytd = j["s_ytd"];
//  v.s_order_cnt = j["s_order_cnt"];
//  v.s_remote_cnt = j["s_remote_cnt"];
  v.s_data = j["s_data"];
  v.amount = j["amount"];
}

void adl_serializer<NewOrderResult>::to_json(nlohmann::json &j, const NewOrderResult &v) {
  throw "Not implemented";
}

void adl_serializer<NewOrderResult>::from_json(const nlohmann::json &j, NewOrderResult &v) {
  v.w_tax_rate = j["w_tax_rate"];
  v.d_tax_rate = j["d_tax_rate"];
  v.d_next_o_id = j["d_next_o_id"];
  v.c_discount = j["c_discount"];
  v.c_last = j["c_last"];
  v.c_credit = j["c_credit"];

  v.order_lines.reserve(j["order_lines"].size());
  for (const auto & ol_j : j["order_lines"]) {
    NewOrderOrderLineResult order_line = ol_j;
    v.order_lines.emplace_back(order_line);
  }
}
}