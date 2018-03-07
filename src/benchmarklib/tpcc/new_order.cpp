#include "new_order.hpp"

#include <boost/variant.hpp>

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "helper.hpp"

#include "abstract_expression.hpp"
#include "concurrency/commit_context.hpp"
#include "concurrency/transaction_context.hpp"
#include "concurrency/transaction_manager.hpp"
#include "operators/get_table.hpp"
#include "operators/insert.hpp"
#include "operators/pqp_expression.hpp"
#include "operators/product.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/update.hpp"
#include "operators/validate.hpp"
#include "scheduler/operator_task.hpp"
#include "storage/storage_manager.hpp"
#include "types.hpp"

#include "all_type_variant.hpp"

namespace tpcc {

NewOrderResult AbstractNewOrderImpl::run_transaction(const NewOrderParams& params) {
  NewOrderResult result;
  std::vector<opossum::AllTypeVariant> row;

  opossum::TransactionManager::get().run_transaction([&](std::shared_ptr<opossum::TransactionContext> t_context) {
    /**
     * GET CUSTOMER AND WAREHOUSE TAX RATE
     */
    auto get_customer_and_warehouse_tax_rate_tasks =
        get_get_customer_and_warehouse_tax_rate_tasks(params.w_id, params.d_id, params.c_id);
    execute_tasks_with_context(get_customer_and_warehouse_tax_rate_tasks, t_context);

    const auto customer_and_warehouse_tax_rate_table =
        get_customer_and_warehouse_tax_rate_tasks.back()->get_operator()->get_output();

    result.c_discount = customer_and_warehouse_tax_rate_table->get_value<float>(opossum::ColumnID(0), 0);
    result.c_last = customer_and_warehouse_tax_rate_table->get_value<std::string>(opossum::ColumnID(1), 0);
    result.c_credit = customer_and_warehouse_tax_rate_table->get_value<std::string>(opossum::ColumnID(2), 0);
    result.w_tax_rate = customer_and_warehouse_tax_rate_table->get_value<float>(opossum::ColumnID(3), 0);

    /**
     * GET DISTRICT
     */
    auto get_district_tasks = get_get_district_tasks(params.d_id, params.w_id);
    execute_tasks_with_context(get_district_tasks, t_context);
    const auto districts_table = get_district_tasks.back()->get_operator()->get_output();

    result.d_next_o_id = districts_table->get_value<int32_t>(opossum::ColumnID(0), 0);
    result.d_tax_rate = districts_table->get_value<float>(opossum::ColumnID(1), 0);

    /**
     * INCREMENT NEXT ORDER ID
     */
    auto increment_next_order_id_tasks =
        get_increment_next_order_id_tasks(params.d_id, params.w_id, result.d_next_o_id);
    execute_tasks_with_context(increment_next_order_id_tasks, t_context);

    /**
     * CREATE ORDER
     */
    auto create_order_tasks = get_create_order_tasks(result.d_next_o_id, params.d_id, params.w_id, params.c_id,
                                                     params.o_entry_d, 0, params.order_lines.size(), 1);
    execute_tasks_with_context(create_order_tasks, t_context);

    /**
     * CREATE NEW ORDER
     */
    auto create_new_order_tasks = get_create_new_order_tasks(result.d_next_o_id, params.d_id, params.w_id);
    execute_tasks_with_context(create_new_order_tasks, t_context);

    for (size_t ol_idx = 0; ol_idx < params.order_lines.size(); ol_idx++) {
      const auto& order_line_params = params.order_lines[ol_idx];

      NewOrderOrderLineResult order_line;

      /**
       * GET ITEM INFO
       */
      auto get_item_info_tasks = get_get_item_info_tasks(order_line_params.i_id);
      execute_tasks_with_context(get_item_info_tasks, t_context);
      const auto item_info_table = get_item_info_tasks.back()->get_operator()->get_output();

      order_line.i_price = item_info_table->get_value<float>(opossum::ColumnID(0), 0);
      order_line.i_name = item_info_table->get_value<std::string>(opossum::ColumnID(1), 0);
      order_line.i_data = item_info_table->get_value<std::string>(opossum::ColumnID(2), 0);

      /**
       * GET STOCK INFO
       */
      auto get_stock_info_tasks =
          get_get_stock_info_tasks(order_line_params.i_id, order_line_params.w_id, params.d_id + 1);
      execute_tasks_with_context(get_stock_info_tasks, t_context);
      const auto stock_info_table = get_stock_info_tasks.back()->get_operator()->get_output();

      order_line.s_qty = stock_info_table->get_value<int32_t>(opossum::ColumnID(0), 0);
      order_line.s_data = stock_info_table->get_value<std::string>(opossum::ColumnID(1), 0);
      order_line.s_ytd = stock_info_table->get_value<int32_t>(opossum::ColumnID(2), 0);
      order_line.s_order_cnt = stock_info_table->get_value<int32_t>(opossum::ColumnID(3), 0);
      order_line.s_remote_cnt = stock_info_table->get_value<int32_t>(opossum::ColumnID(4), 0);
      order_line.s_dist_xx = stock_info_table->get_value<std::string>(opossum::ColumnID(5), 0);

      /**
       * Calculate new s_ytd, s_qty and s_order_cnt
       */
      // auto s_ytd = order_line.s_ytd + order_line_params.qty;
      // TODO(anybody): why doesn't the tpc ref impl update this in UPDATE STOCK?

      if (order_line.s_qty >= order_line_params.qty + 10) {
        order_line.s_qty -= order_line_params.qty;
      } else {
        order_line.s_qty += 91 - order_line_params.qty;
      }

      // auto s_order_cnt = order_line.s_order_cnt + 1;
      // TODO(anybody): why doesn't the tpc ref impl update this in UPDATE STOCK?
      order_line.amount = order_line_params.qty * order_line.i_price;

      /**
       * UPDATE STOCK
       */
      auto update_stock_tasks =
          get_update_stock_tasks(order_line.s_qty, order_line_params.i_id, order_line_params.w_id);
      execute_tasks_with_context(update_stock_tasks, t_context);

      /**
       * CREATE ORDER LINE
       */
      auto create_order_line_tasks =
          get_create_order_line_tasks(result.d_next_o_id, params.d_id, params.w_id, ol_idx + 1, order_line_params.i_id,
                                      0,  // ol_supply_w_id - we only have one warehouse
                                      params.o_entry_d, order_line_params.qty, order_line.amount, order_line.s_dist_xx);
      execute_tasks_with_context(create_order_line_tasks, t_context);

      /**
       * Add results
       */
      result.order_lines.emplace_back(order_line);
    }
  });

  return result;
}

TaskVector NewOrderRefImpl::get_get_customer_and_warehouse_tax_rate_tasks(const int32_t w_id, const int32_t d_id,
                                                                          const int32_t c_id) {
  /**
   * SELECT c_discount, c_last, c_credit, w_tax
   * FROM customer, warehouse
   * WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id
   */

  // Operators
  const auto c_gt = std::make_shared<opossum::GetTable>("CUSTOMER");
  const auto c_v = std::make_shared<opossum::Validate>(c_gt);

  const auto c_ts1 = std::make_shared<opossum::TableScan>(c_v, opossum::ColumnID{2} /* "C_W_ID" */,
                                                          opossum::PredicateCondition::Equals, w_id);

  const auto c_ts2 = std::make_shared<opossum::TableScan>(c_ts1, opossum::ColumnID{1} /* "C_D_ID" */,
                                                          opossum::PredicateCondition::Equals, d_id);

  const auto c_ts3 = std::make_shared<opossum::TableScan>(c_ts2, opossum::ColumnID{0} /* "C_ID" */,
                                                          opossum::PredicateCondition::Equals, c_id);

  const auto w_gt = std::make_shared<opossum::GetTable>("WAREHOUSE");
  const auto w_ts = std::make_shared<opossum::TableScan>(w_gt, opossum::ColumnID{0} /* "W_ID" */,
                                                         opossum::PredicateCondition::Equals, w_id);

  // Both operators should have exactly one row -> Product operator should have smallest overhead.
  const auto join = std::make_shared<opossum::Product>(c_ts3, w_ts);

  const auto proj = std::make_shared<opossum::Projection>(
      join, opossum::Projection::ColumnExpressions(
                {opossum::PQPExpression::create_column(opossum::ColumnID{15} /* "C_DISCOUNT" */),
                 opossum::PQPExpression::create_column(opossum::ColumnID{5} /* "C_LAST" */),
                 opossum::PQPExpression::create_column(opossum::ColumnID{13} /* "C_CREDIT" */),
                 opossum::PQPExpression::create_column(opossum::ColumnID{28} /* "W_TAX" */)}));

  // Tasks
  const auto c_gt_t = std::make_shared<opossum::OperatorTask>(c_gt);
  const auto c_v_t = std::make_shared<opossum::OperatorTask>(c_v);
  const auto c_ts1_t = std::make_shared<opossum::OperatorTask>(c_ts1);
  const auto c_ts2_t = std::make_shared<opossum::OperatorTask>(c_ts2);
  const auto c_ts3_t = std::make_shared<opossum::OperatorTask>(c_ts3);

  const auto w_gt_t = std::make_shared<opossum::OperatorTask>(w_gt);
  const auto w_ts_t = std::make_shared<opossum::OperatorTask>(w_ts);

  const auto join_t = std::make_shared<opossum::OperatorTask>(join);
  const auto proj_t = std::make_shared<opossum::OperatorTask>(proj);

  // Dependencies
  c_gt_t->set_as_predecessor_of(c_v_t);
  c_v_t->set_as_predecessor_of(c_ts1_t);
  c_ts1_t->set_as_predecessor_of(c_ts2_t);
  c_ts2_t->set_as_predecessor_of(c_ts3_t);

  w_gt_t->set_as_predecessor_of(w_ts_t);

  c_ts3_t->set_as_predecessor_of(join_t);
  w_ts_t->set_as_predecessor_of(join_t);

  join_t->set_as_predecessor_of(proj_t);

  return {c_gt_t, c_v_t, c_ts1_t, c_ts2_t, c_ts3_t, w_gt_t, w_ts_t, join_t, proj_t};
}

TaskVector NewOrderRefImpl::get_get_district_tasks(const int32_t d_id, const int32_t w_id) {
  /**
   * SELECT d_next_o_id, d_tax
   * FROM district
   * WHERE d_id = :d_id AND d_w_id = :w_id
   */

  // Operators
  const auto gt = std::make_shared<opossum::GetTable>("DISTRICT");
  const auto v = std::make_shared<opossum::Validate>(gt);

  const auto ts1 = std::make_shared<opossum::TableScan>(v, opossum::ColumnID{0} /* "D_ID" */,
                                                        opossum::PredicateCondition::Equals, d_id);
  const auto ts2 = std::make_shared<opossum::TableScan>(ts1, opossum::ColumnID{1} /* "D_W_ID" */,
                                                        opossum::PredicateCondition::Equals, w_id);

  const auto proj = std::make_shared<opossum::Projection>(
      ts2, opossum::Projection::ColumnExpressions(
               {opossum::PQPExpression::create_column(opossum::ColumnID{10} /* "D_NEXT_O_ID" */),
                opossum::PQPExpression::create_column(opossum::ColumnID{8} /* "D_TAX" */)}));

  // Tasks
  const auto gt_t = std::make_shared<opossum::OperatorTask>(gt);
  const auto v_t = std::make_shared<opossum::OperatorTask>(v);
  const auto ts1_t = std::make_shared<opossum::OperatorTask>(ts1);
  const auto ts2_t = std::make_shared<opossum::OperatorTask>(ts2);
  const auto proj_t = std::make_shared<opossum::OperatorTask>(proj);

  // Dependencies
  gt_t->set_as_predecessor_of(v_t);
  v_t->set_as_predecessor_of(ts1_t);
  ts1_t->set_as_predecessor_of(ts2_t);
  ts2_t->set_as_predecessor_of(proj_t);

  return {gt_t, v_t, ts1_t, ts2_t, proj_t};
}

TaskVector NewOrderRefImpl::get_increment_next_order_id_tasks(const int32_t d_id, const int32_t d_w_id,
                                                              const int32_t d_next_o_id) {
  /**
   * UPDATE district
   * SET d_next_o_id = :d_next_o_id + 1
   * WHERE d_id = :d_id AND d_w_id = :w_id
   */

  // Operators
  const auto gt = std::make_shared<opossum::GetTable>("DISTRICT");
  const auto v = std::make_shared<opossum::Validate>(gt);

  const auto ts1 = std::make_shared<opossum::TableScan>(v, opossum::ColumnID{0} /* "D_ID" */,
                                                        opossum::PredicateCondition::Equals, d_id);
  const auto ts2 = std::make_shared<opossum::TableScan>(ts1, opossum::ColumnID{1} /* "D_W_ID" */,
                                                        opossum::PredicateCondition::Equals, d_w_id);

  const auto original_rows = std::make_shared<opossum::Projection>(
      ts2, opossum::Projection::ColumnExpressions({opossum::PQPExpression::create_column(opossum::ColumnID{10})}));

  const auto op = opossum::PQPExpression::create_binary_operator(opossum::ExpressionType::Addition,
                                                                 opossum::PQPExpression::create_literal(d_next_o_id),
                                                                 opossum::PQPExpression::create_literal(1), {"fix"});
  const auto updated_rows = std::make_shared<opossum::Projection>(ts2, opossum::Projection::ColumnExpressions{op});

  const auto update = std::make_shared<opossum::Update>("DISTRICT", original_rows, updated_rows);

  // Tasks
  const auto gt_t = std::make_shared<opossum::OperatorTask>(gt);
  const auto v_t = std::make_shared<opossum::OperatorTask>(v);
  const auto ts1_t = std::make_shared<opossum::OperatorTask>(ts1);
  const auto ts2_t = std::make_shared<opossum::OperatorTask>(ts2);
  const auto original_rows_t = std::make_shared<opossum::OperatorTask>(original_rows);
  const auto updated_rows_t = std::make_shared<opossum::OperatorTask>(updated_rows);
  const auto update_t = std::make_shared<opossum::OperatorTask>(update);

  // Dependencies
  gt_t->set_as_predecessor_of(v_t);
  v_t->set_as_predecessor_of(ts1_t);
  ts1_t->set_as_predecessor_of(ts2_t);

  ts2_t->set_as_predecessor_of(original_rows_t);
  ts2_t->set_as_predecessor_of(updated_rows_t);

  original_rows_t->set_as_predecessor_of(update_t);
  updated_rows_t->set_as_predecessor_of(update_t);

  return {gt_t, v_t, ts1_t, ts2_t, original_rows_t, updated_rows_t, update_t};
}

TaskVector NewOrderRefImpl::get_create_order_tasks(const int32_t d_next_o_id, const int32_t d_id, const int32_t w_id,
                                                   const int32_t c_id, const int32_t o_entry_d,
                                                   const int32_t o_carrier_id, const int32_t o_ol_cnt,
                                                   const int32_t o_all_local) {
  /**
   * INSERT INTO ORDER (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
   * VALUES (?, ?, ?, ?, ?, ?, ?, ?)
   */

  auto target_table_name = std::string("ORDER");
  const auto original_table = opossum::StorageManager::get().get_table(target_table_name);

  opossum::TableColumnDefinitions column_definitions;
  for (opossum::ColumnID columnID{0}; columnID < original_table->column_count(); columnID++) {
    column_definitions.emplace_back(original_table->column_name(columnID), original_table->column_data_type(columnID),
                                    false);
  }
  auto new_table = std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data);

  opossum::ChunkColumns columns;
  columns.push_back(create_single_value_column<int32_t>(d_next_o_id));
  columns.push_back(create_single_value_column<int32_t>(d_id));
  columns.push_back(create_single_value_column<int32_t>(w_id));
  columns.push_back(create_single_value_column<int32_t>(c_id));
  columns.push_back(create_single_value_column<int32_t>(o_entry_d));
  columns.push_back(create_single_value_column<int32_t>(o_carrier_id));
  columns.push_back(create_single_value_column<int32_t>(o_ol_cnt));
  columns.push_back(create_single_value_column<int32_t>(o_all_local));
  new_table->append_chunk(columns);

  auto tw = std::make_shared<opossum::TableWrapper>(new_table);
  const auto insert = std::make_shared<opossum::Insert>(target_table_name, tw);

  const auto tw_t = std::make_shared<opossum::OperatorTask>(tw);
  const auto insert_t = std::make_shared<opossum::OperatorTask>(insert);

  return {tw_t, insert_t};
}

TaskVector NewOrderRefImpl::get_create_new_order_tasks(const int32_t o_id, const int32_t d_id, const int32_t w_id) {
  /**
   * INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
   * VALUES (?, ?, ?);
   */

  auto target_table_name = std::string("NEW_ORDER");
  const auto original_table = opossum::StorageManager::get().get_table(target_table_name);

  opossum::TableColumnDefinitions column_definitions;
  for (opossum::ColumnID columnID{0}; columnID < original_table->column_count(); columnID++) {
    column_definitions.emplace_back(original_table->column_name(columnID), original_table->column_data_type(columnID),
                                    false);
  }
  auto new_table = std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data);

  opossum::ChunkColumns columns;
  columns.push_back(create_single_value_column<int32_t>(o_id));
  columns.push_back(create_single_value_column<int32_t>(d_id));
  columns.push_back(create_single_value_column<int32_t>(w_id));
  new_table->append_chunk(columns);

  auto tw = std::make_shared<opossum::TableWrapper>(new_table);
  const auto insert = std::make_shared<opossum::Insert>(target_table_name, tw);

  const auto tw_t = std::make_shared<opossum::OperatorTask>(tw);
  const auto insert_t = std::make_shared<opossum::OperatorTask>(insert);

  return {tw_t, insert_t};
}

TaskVector NewOrderRefImpl::get_get_item_info_tasks(const int32_t ol_i_id) {
  /**
   * SELECT i_price, i_name , i_data
   * FROM item
   * WHERE i_id = :ol_i_id;
   */

  // Operators
  const auto gt = std::make_shared<opossum::GetTable>("ITEM");
  const auto v = std::make_shared<opossum::Validate>(gt);

  //  "I_ID"
  const auto ts =
      std::make_shared<opossum::TableScan>(v, opossum::ColumnID{0}, opossum::PredicateCondition::Equals, ol_i_id);

  const auto proj = std::make_shared<opossum::Projection>(
      ts, opossum::Projection::ColumnExpressions({opossum::PQPExpression::create_column(opossum::ColumnID{3}),
                                                  opossum::PQPExpression::create_column(opossum::ColumnID{2}),
                                                  opossum::PQPExpression::create_column(opossum::ColumnID{4})}));

  // Tasks
  auto gt_t = std::make_shared<opossum::OperatorTask>(gt);
  auto v_t = std::make_shared<opossum::OperatorTask>(v);
  auto ts_t = std::make_shared<opossum::OperatorTask>(ts);
  auto proj_t = std::make_shared<opossum::OperatorTask>(proj);

  // Dependencies
  gt_t->set_as_predecessor_of(v_t);
  v_t->set_as_predecessor_of(ts_t);
  ts_t->set_as_predecessor_of(proj_t);

  return {gt_t, v_t, ts_t, proj_t};
}

TaskVector NewOrderRefImpl::get_get_stock_info_tasks(const int32_t ol_i_id, const int32_t ol_supply_w_id,
                                                     const int32_t d_id) {
  /**
   * SELECT
   *  s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt
   *  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05 s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
   * FROM stock
   * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
   */

  // Operators
  const auto gt = std::make_shared<opossum::GetTable>("STOCK");
  const auto v = std::make_shared<opossum::Validate>(gt);

  const auto ts1 = std::make_shared<opossum::TableScan>(v, opossum::ColumnID{0} /* "S_I_ID" */,
                                                        opossum::PredicateCondition::Equals, ol_i_id);
  const auto ts2 = std::make_shared<opossum::TableScan>(ts1, opossum::ColumnID{1} /* "S_W_ID" */,
                                                        opossum::PredicateCondition::Equals, ol_supply_w_id);

  std::string s_dist_xx = d_id < 10 ? "S_DIST_0" + std::to_string(d_id) : "S_DIST_" + std::to_string(d_id);

  const auto proj = std::make_shared<opossum::Projection>(
      ts2, opossum::Projection::ColumnExpressions(
               {opossum::PQPExpression::create_column(opossum::ColumnID{2} /* "S_QUANTITY" */),
                opossum::PQPExpression::create_column(opossum::ColumnID{16} /* "S_DATA" */),
                opossum::PQPExpression::create_column(opossum::ColumnID{13} /* "S_YTD" */),
                opossum::PQPExpression::create_column(opossum::ColumnID{14} /* "S_ORDER_CNT" */),
                opossum::PQPExpression::create_column(opossum::ColumnID{15} /* "S_REMOTE_CNT" */),
                opossum::PQPExpression::create_column(
                    opossum::ColumnID{static_cast<opossum::ColumnID::base_type>(d_id + 2)} /* s_dist_xx */)}));

  // Tasks
  auto gt_t = std::make_shared<opossum::OperatorTask>(gt);
  auto v_t = std::make_shared<opossum::OperatorTask>(v);
  auto ts1_t = std::make_shared<opossum::OperatorTask>(ts1);
  auto ts2_t = std::make_shared<opossum::OperatorTask>(ts2);
  auto proj_t = std::make_shared<opossum::OperatorTask>(proj);

  // Dependencies
  gt_t->set_as_predecessor_of(v_t);
  v_t->set_as_predecessor_of(ts1_t);
  ts1_t->set_as_predecessor_of(ts2_t);
  ts2_t->set_as_predecessor_of(proj_t);

  return {gt_t, v_t, ts1_t, ts2_t, proj_t};
}

TaskVector NewOrderRefImpl::get_update_stock_tasks(const int32_t s_quantity, const int32_t ol_i_id,
                                                   const int32_t ol_supply_w_id) {
  /**
   * TODO(anybody) unlike Pavlo, this doesn't update ytd, remote_cnt, order_cnt
   *
   * UPDATE stock
   * SET s_quantity = :s_quantity
   * WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id
   */

  // Operators
  const auto gt = std::make_shared<opossum::GetTable>("STOCK");
  const auto v = std::make_shared<opossum::Validate>(gt);

  const auto ts1 = std::make_shared<opossum::TableScan>(v, opossum::ColumnID{0} /* "S_I_ID" */,
                                                        opossum::PredicateCondition::Equals, ol_i_id);

  const auto ts2 = std::make_shared<opossum::TableScan>(ts1, opossum::ColumnID{1} /* "S_W_ID" */,
                                                        opossum::PredicateCondition::Equals, ol_supply_w_id);

  const auto original_rows = std::make_shared<opossum::Projection>(
      ts2, opossum::Projection::ColumnExpressions({opossum::PQPExpression::create_column(opossum::ColumnID{2})}));

  const auto updated_rows = std::make_shared<opossum::Projection>(
      ts2, opossum::Projection::ColumnExpressions({opossum::PQPExpression::create_literal(s_quantity, {"fix"})}));

  const auto update = std::make_shared<opossum::Update>("STOCK", original_rows, updated_rows);

  // Tasks
  const auto gt_t = std::make_shared<opossum::OperatorTask>(gt);
  const auto v_t = std::make_shared<opossum::OperatorTask>(v);
  const auto ts1_t = std::make_shared<opossum::OperatorTask>(ts1);
  const auto ts2_t = std::make_shared<opossum::OperatorTask>(ts2);
  const auto original_rows_t = std::make_shared<opossum::OperatorTask>(original_rows);
  const auto updated_rows_t = std::make_shared<opossum::OperatorTask>(updated_rows);
  const auto update_t = std::make_shared<opossum::OperatorTask>(update);

  // Dependencies
  gt_t->set_as_predecessor_of(v_t);
  v_t->set_as_predecessor_of(ts1_t);
  ts1_t->set_as_predecessor_of(ts2_t);

  ts2_t->set_as_predecessor_of(original_rows_t);
  ts2_t->set_as_predecessor_of(updated_rows_t);

  original_rows_t->set_as_predecessor_of(update_t);
  updated_rows_t->set_as_predecessor_of(update_t);

  return {gt_t, v_t, ts1_t, ts2_t, original_rows_t, updated_rows_t, update_t};
}

TaskVector NewOrderRefImpl::get_create_order_line_tasks(const int32_t ol_o_id, const int32_t ol_d_id,
                                                        const int32_t ol_w_id, const int32_t ol_number,
                                                        const int32_t ol_i_id, const int32_t ol_supply_w_id,
                                                        const int32_t ol_delivery_d, const int32_t ol_quantity,
                                                        const float ol_amount, const std::string& ol_dist_info) {
  /**
   *  INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number,
   *  ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
   *  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
   */

  auto target_table_name = std::string("ORDER_LINE");
  const auto original_table = opossum::StorageManager::get().get_table(target_table_name);

  opossum::TableColumnDefinitions column_definitions;
  for (opossum::ColumnID columnID{0}; columnID < original_table->column_count(); columnID++) {
    column_definitions.emplace_back(original_table->column_name(columnID), original_table->column_data_type(columnID),
                                    false);
  }
  auto new_table = std::make_shared<opossum::Table>(column_definitions, opossum::TableType::Data);

  opossum::ChunkColumns columns;
  columns.push_back(create_single_value_column<int32_t>(ol_o_id));
  columns.push_back(create_single_value_column<int32_t>(ol_d_id));
  columns.push_back(create_single_value_column<int32_t>(ol_w_id));
  columns.push_back(create_single_value_column<int32_t>(ol_number));
  columns.push_back(create_single_value_column<int32_t>(ol_i_id));
  columns.push_back(create_single_value_column<int32_t>(ol_supply_w_id));
  columns.push_back(create_single_value_column<int32_t>(ol_delivery_d));
  columns.push_back(create_single_value_column<int32_t>(ol_quantity));
  columns.push_back(create_single_value_column<float>(ol_amount));
  columns.push_back(create_single_value_column<std::string>(ol_dist_info));
  new_table->append_chunk(columns);

  auto tw = std::make_shared<opossum::TableWrapper>(new_table);
  const auto insert = std::make_shared<opossum::Insert>(target_table_name, tw);

  const auto tw_t = std::make_shared<opossum::OperatorTask>(tw);
  const auto insert_t = std::make_shared<opossum::OperatorTask>(insert);

  return {tw_t, insert_t};
}

}  // namespace tpcc

namespace nlohmann {

void adl_serializer<tpcc::NewOrderParams>::to_json(nlohmann::json& j, const tpcc::NewOrderParams& v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::NewOrderParams>::from_json(const nlohmann::json& j, tpcc::NewOrderParams& v) {
  v.w_id = j["w_id"];
  v.d_id = j["d_id"];
  v.c_id = j["c_id"];
  v.o_entry_d = j["o_entry_d"];

  v.order_lines.reserve(j["order_lines"].size());
  for (const auto& ol_j : j["order_lines"]) {
    tpcc::NewOrderOrderLineParams order_line_params = ol_j;
    v.order_lines.emplace_back(order_line_params);
  }
}

void adl_serializer<tpcc::NewOrderOrderLineParams>::to_json(nlohmann::json& j, const tpcc::NewOrderOrderLineParams& v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::NewOrderOrderLineParams>::from_json(const nlohmann::json& j,
                                                              tpcc::NewOrderOrderLineParams& v) {
  v.i_id = j["i_id"];
  v.w_id = j["w_id"];
  v.qty = j["qty"];
}

void adl_serializer<tpcc::NewOrderOrderLineResult>::to_json(nlohmann::json& j, const tpcc::NewOrderOrderLineResult& v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::NewOrderOrderLineResult>::from_json(const nlohmann::json& j,
                                                              tpcc::NewOrderOrderLineResult& v) {
  v.i_price = j["i_price"];
  v.i_name = j["i_name"];
  v.i_data = j["i_data"];
  v.s_qty = j["s_qty"];
  v.s_dist_xx = j["s_dist_xx"];
  // TODO(anybody): don't use these until they are updated in STOCK UPDATE.
  // v.s_ytd = j["s_ytd"];
  // v.s_order_cnt = j["s_order_cnt"];
  // v.s_remote_cnt = j["s_remote_cnt"];
  v.s_data = j["s_data"];
  v.amount = j["amount"];
}

void adl_serializer<tpcc::NewOrderResult>::to_json(nlohmann::json& j, const tpcc::NewOrderResult& v) {
  throw "Not implemented";
}

void adl_serializer<tpcc::NewOrderResult>::from_json(const nlohmann::json& j, tpcc::NewOrderResult& v) {
  v.w_tax_rate = j["w_tax_rate"];
  v.d_tax_rate = j["d_tax_rate"];
  v.d_next_o_id = j["d_next_o_id"];
  v.c_discount = j["c_discount"];
  v.c_last = j["c_last"];
  v.c_credit = j["c_credit"];

  v.order_lines.reserve(j["order_lines"].size());
  for (const auto& ol_j : j["order_lines"]) {
    tpcc::NewOrderOrderLineResult order_line = ol_j;
    v.order_lines.emplace_back(order_line);
  }
}
}  // namespace nlohmann
