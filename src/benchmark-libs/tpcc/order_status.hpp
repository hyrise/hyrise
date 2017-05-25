#pragma once

#include <stdint.h>
#include <string>

#include <json.hpp>

#include "../../lib/scheduler/operator_task.hpp"

#include "abstract_transaction_impl.h"

namespace tpcc {

enum class OrderStatusBy {
  CustomerNumber, CustomerLastName
};

struct OrderStatusParams {
  uint32_t c_w_id = 0;
  uint32_t c_d_id = 0;
  OrderStatusBy order_status_by = OrderStatusBy::CustomerNumber;
  uint32_t c_id = 0;
  std::string c_last;
};

struct OrderStatusOrderLine {
  uint32_t ol_supply_w_id = 0;
  uint32_t ol_i_id = 0;
  uint32_t ol_quantity = 0;
  float ol_amount = 0.0f;
  std::string ol_delivery_d;
};

struct OrderStatusResult {
  uint32_t c_id = 0;
  std::string c_first;
  std::string c_middle;
  std::string c_last;
  float c_balance = 0.0f;
  uint32_t o_id = 0;
  uint32_t o_carrier_id = 0;
  std::string o_entry_d;
  std::vector<OrderStatusOrderLine> order_lines;
};

class AbstractOrderStatusQueryImpl : public opossum::AbstractTransactionImpl {
 public:
  virtual std::vector <std::shared_ptr<OperatorTask>>
  get_customer_by_name(const std::string c_last, const int c_d_id,
                       const int c_w_id) = 0;

  virtual std::vector <std::shared_ptr<OperatorTask>>
  get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) = 0;

  virtual std::vector <std::shared_ptr<OperatorTask>> get_orders() = 0;

  virtual std::vector <std::shared_ptr<OperatorTask>>
  get_order_lines(const int o_id, const int d_id, const int w_id) = 0;

  OrderStatusResult run_transaction(const OrderStatusParams & params);

  void run_and_test_transaction_from_json(const nlohmann::json & json_params,
                                          const nlohmann::json & json_results) override;
};



}
