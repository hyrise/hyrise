#pragma once

#include <json.hpp>

#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "defines.hpp"

namespace opossum {

class TransactionContext;

}  // namespace opossum

namespace tpcc {

enum class OrderStatusBy { CustomerNumber, CustomerLastName };

struct OrderStatusParams {
  int32_t c_w_id = 0;
  int32_t c_d_id = 0;
  OrderStatusBy order_status_by = OrderStatusBy::CustomerNumber;
  uint32_t c_id = 0;
  std::string c_last;

  std::string to_string() const;
};

struct OrderStatusOrderLine {
  int32_t ol_supply_w_id = 0;
  int32_t ol_i_id = 0;
  int32_t ol_quantity = 0;
  float ol_amount = 0.0f;
  int32_t ol_delivery_d;
};

struct OrderStatusResult {
  int32_t c_id = 0;
  std::string c_first;
  std::string c_middle;
  std::string c_last;
  float c_balance = 0.0f;
  int32_t o_id = 0;
  int32_t o_carrier_id = 0;
  int32_t o_entry_d;
  std::vector<OrderStatusOrderLine> order_lines;
};

class AbstractOrderStatusImpl {
 public:
  virtual TaskVector get_customer_by_name(const std::string c_last, const int c_d_id, const int c_w_id) = 0;
  virtual TaskVector get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) = 0;

  virtual TaskVector get_orders(const int c_id, const int c_d_id, const int c_w_id) = 0;
  virtual TaskVector get_order_lines(const int o_id, const int d_id, const int w_id) = 0;

  OrderStatusResult run_transaction(const OrderStatusParams &params);
};

class OrderStatusRefImpl : public AbstractOrderStatusImpl {
 public:
  TaskVector get_customer_by_name(const std::string c_last, const int c_d_id, const int c_w_id) override;
  TaskVector get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) override;

  TaskVector get_orders(const int c_id, const int c_d_id, const int c_w_id) override;
  TaskVector get_order_lines(const int o_id, const int d_id, const int w_id) override;
};

}  // namespace tpcc

namespace nlohmann {

template <>
struct adl_serializer<tpcc::OrderStatusParams> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusParams &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusParams &v);
};

template <>
struct adl_serializer<tpcc::OrderStatusOrderLine> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusOrderLine &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusOrderLine &v);
};

template <>
struct adl_serializer<tpcc::OrderStatusResult> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusResult &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusResult &v);
};

}  // namespace nlohmann
