#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "defines.h"

namespace tpcc {

struct NewOrderOrderLineParams {
  int32_t i_id;
  int32_t w_id;
  int32_t qty;
};

struct NewOrderParams {
  int32_t w_id;
  int32_t d_id;
  int32_t c_id;
  int32_t o_entry_d;
  std::vector<NewOrderOrderLineParams> order_lines;
};

struct NewOrderOrderLineResult {
  float i_price;
  std::string i_name;
  std::string i_data;
  int32_t s_qty;
  std::string s_dist_xx;
  int32_t s_ytd;
  int32_t s_order_cnt;
  int32_t s_remove_cnt;
  std::string s_data;
};

struct NewOrderResult {
  float w_tax_rate;
  float d_tax_rate;
  int32_t d_next_o_id;
  float c_discount;
  std::string c_last;
  float c_credit;
  std::vector<NewOrderOrderLineResult> order_lines;
};

class AbstractNewOrderImpl {
  virtual TaskVector get_get_customer_and_warehouse_tax_rate_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t w_id, const int32_t d_id,
    const int32_t c_id) = 0;

  virtual TaskVector
  get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
                         const int32_t d_id, const int32_t w_id) = 0;

  virtual TaskVector get_increment_next_order_id_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t d_id, const int32_t d_w_id,
    const int32_t d_next_o_id) = 0;

  virtual TaskVector get_create_order_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t d_next_o_id, const int32_t d_id, const
  int32_t w_id, const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id, const int32_t
    o_ol_cnt, const int32_t o_all_local) = 0;

  virtual TaskVector get_create_new_order_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t o_id, const int32_t d_id, const int32_t
  w_id) = 0;

  virtual TaskVector get_get_item_info_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id) = 0;

  virtual TaskVector get_get_stock_info_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id, const int32_t ol_supply_w_id) = 0;

  virtual TaskVector
  get_update_stock_tasks(const std::shared_ptr<TransactionContext> t_context,
                         const int32_t s_quantity, const int32_t ol_i_id,
                         const int32_t ol_supply_w_id) = 0;
  
  NewOrderResult run_transaction(const NewOrderParams & params);
};

class NewOrderRefImpl : public AbstractNewOrderImpl {
  TaskVector get_get_customer_and_warehouse_tax_rate_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t w_id, const int32_t d_id,
    const int32_t c_id) override;

  TaskVector
  get_get_district_tasks(const std::shared_ptr<TransactionContext> t_context,
                         const int32_t d_id, const int32_t w_id) override;

  TaskVector get_increment_next_order_id_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t d_id, const int32_t d_w_id,
    const int32_t d_next_o_id) override;

  TaskVector get_create_order_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t d_next_o_id, const int32_t d_id, const
  int32_t w_id, const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id, const int32_t
    o_ol_cnt, const int32_t o_all_local) override;

  TaskVector get_create_new_order_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t o_id, const int32_t d_id, const int32_t
  w_id) override;

  TaskVector get_get_item_info_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id) override;

  TaskVector get_get_stock_info_tasks(
    const std::shared_ptr<TransactionContext> t_context, const int32_t ol_i_id, const int32_t ol_supply_w_id) override;

  TaskVector
  get_update_stock_tasks(const std::shared_ptr<TransactionContext> t_context,
                         const int32_t s_quantity, const int32_t ol_i_id,
                         const int32_t ol_supply_w_id) override;
};

}