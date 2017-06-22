#pragma once

#include <json.hpp>

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "defines.hpp"

#include "concurrency/transaction_context.hpp"

namespace tpcc {

struct NewOrderOrderLineParams {
  int32_t i_id = 0;
  int32_t w_id = 0;
  int32_t qty = 0;
};

struct NewOrderParams {
  int32_t w_id = 0;
  int32_t d_id = 0;
  int32_t c_id = 0;
  int32_t o_entry_d = 0;
  std::vector<NewOrderOrderLineParams> order_lines;

  std::string to_string() const;
};

struct NewOrderOrderLineResult {
  float i_price = 0.0f;
  std::string i_name;
  std::string i_data;
  int32_t s_qty = 0;
  std::string s_dist_xx;
  int32_t s_ytd = 0;
  int32_t s_order_cnt = 0;
  int32_t s_remote_cnt = 0;
  std::string s_data;
  float amount = 0.0f;
};

struct NewOrderResult {
  float w_tax_rate = 0.0f;
  float d_tax_rate = 0.0f;
  int32_t d_next_o_id = 0;
  float c_discount = 0.0f;
  std::string c_last;
  std::string c_credit;
  std::vector<NewOrderOrderLineResult> order_lines;
};

class AbstractNewOrderImpl {
 public:
  virtual TaskVector get_get_customer_and_warehouse_tax_rate_tasks(
      const std::shared_ptr<opossum::TransactionContext> t_context, const int32_t w_id, const int32_t d_id,
      const int32_t c_id) = 0;

  virtual TaskVector get_get_district_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                            const int32_t d_id, const int32_t w_id) = 0;

  virtual TaskVector get_increment_next_order_id_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                                       const int32_t d_id, const int32_t d_w_id,
                                                       const int32_t d_next_o_id) = 0;

  virtual TaskVector get_create_order_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                            const int32_t d_next_o_id, const int32_t d_id, const int32_t w_id,
                                            const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id,
                                            const int32_t o_ol_cnt, const int32_t o_all_local) = 0;

  virtual TaskVector get_create_new_order_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                                const int32_t o_id, const int32_t d_id, const int32_t w_id) = 0;

  virtual TaskVector get_get_item_info_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                             const int32_t ol_i_id) = 0;

  virtual TaskVector get_get_stock_info_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                              const int32_t ol_i_id, const int32_t ol_supply_w_id,
                                              const int32_t d_id) = 0;

  virtual TaskVector get_update_stock_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                            const int32_t s_quantity, const int32_t ol_i_id,
                                            const int32_t ol_supply_w_id) = 0;

  virtual TaskVector get_create_order_line_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                                 const int32_t ol_o_id, const int32_t ol_d_id, const int32_t ol_w_id,
                                                 const int32_t ol_number, const int32_t ol_i_id,
                                                 const int32_t ol_supply_w_id, const int32_t ol_delivery_d,
                                                 const int32_t ol_quantity, const float ol_amount,
                                                 const std::string &ol_dist_info) = 0;

  NewOrderResult run_transaction(const NewOrderParams &params);
};

class NewOrderRefImpl : public AbstractNewOrderImpl {
 public:
  TaskVector get_get_customer_and_warehouse_tax_rate_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                                           const int32_t w_id, const int32_t d_id,
                                                           const int32_t c_id) override;

  TaskVector get_get_district_tasks(const std::shared_ptr<opossum::TransactionContext> t_context, const int32_t d_id,
                                    const int32_t w_id) override;

  TaskVector get_increment_next_order_id_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                               const int32_t d_id, const int32_t d_w_id,
                                               const int32_t d_next_o_id) override;

  TaskVector get_create_order_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                    const int32_t d_next_o_id, const int32_t d_id, const int32_t w_id,
                                    const int32_t c_id, const int32_t o_entry_d, const int32_t o_carrier_id,
                                    const int32_t o_ol_cnt, const int32_t o_all_local) override;

  TaskVector get_create_new_order_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                        const int32_t o_id, const int32_t d_id, const int32_t w_id) override;

  TaskVector get_get_item_info_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                     const int32_t ol_i_id) override;

  TaskVector get_get_stock_info_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                      const int32_t ol_i_id, const int32_t ol_supply_w_id, const int32_t d_id) override;

  TaskVector get_update_stock_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                    const int32_t s_quantity, const int32_t ol_i_id,
                                    const int32_t ol_supply_w_id) override;

  TaskVector get_create_order_line_tasks(const std::shared_ptr<opossum::TransactionContext> t_context,
                                         const int32_t ol_o_id, const int32_t ol_d_id, const int32_t ol_w_id,
                                         const int32_t ol_number, const int32_t ol_i_id, const int32_t ol_supply_w_id,
                                         const int32_t ol_delivery_d, const int32_t ol_quantity, const float ol_amount,
                                         const std::string &ol_dist_info) override;
};

}  // namespace tpcc

namespace nlohmann {
template <>
struct adl_serializer<tpcc::NewOrderOrderLineParams> {
  static void to_json(nlohmann::json &j, const tpcc::NewOrderOrderLineParams &v);
  static void from_json(const nlohmann::json &j, tpcc::NewOrderOrderLineParams &v);
};

template <>
struct adl_serializer<tpcc::NewOrderParams> {
  static void to_json(nlohmann::json &j, const tpcc::NewOrderParams &v);
  static void from_json(const nlohmann::json &j, tpcc::NewOrderParams &v);
};

template <>
struct adl_serializer<tpcc::NewOrderOrderLineResult> {
  static void to_json(nlohmann::json &j, const tpcc::NewOrderOrderLineResult &v);
  static void from_json(const nlohmann::json &j, tpcc::NewOrderOrderLineResult &v);
};

template <>
struct adl_serializer<tpcc::NewOrderResult> {
  static void to_json(nlohmann::json &j, const tpcc::NewOrderResult &v);
  static void from_json(const nlohmann::json &j, tpcc::NewOrderResult &v);
};
}  // namespace nlohmann
