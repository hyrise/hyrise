#pragma once

#include <stdint.h>
#include <string>

#include <json.hpp>

#include "../../lib/scheduler/operator_task.hpp"

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

class AbstractOrderStatusImpl {
 public:
  virtual std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_customer_by_name(const std::string c_last, const int c_d_id,
                       const int c_w_id) = 0;
  virtual std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) = 0;
  virtual std::vector<std::shared_ptr<opossum::OperatorTask>> get_orders() = 0;
  virtual std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_order_lines(const int o_id, const int d_id, const int w_id) = 0;

  OrderStatusResult run_transaction(const OrderStatusParams & params);
};

class OrderStatusRefImpl : public AbstractOrderStatusImpl
{
 public:
  std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_customer_by_name(const std::string c_last, const int c_d_id,
                       const int c_w_id) override;
  std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_customer_by_id(const int c_id, const int c_d_id, const int c_w_id) override;
  std::vector<std::shared_ptr<opossum::OperatorTask>> get_orders() override;
  std::vector<std::shared_ptr<opossum::OperatorTask>>
  get_order_lines(const int o_id, const int d_id, const int w_id) override;
};

}

namespace nlohmann
{
template<>
struct adl_serializer<tpcc::OrderStatusParams> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusParams &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusParams &v);
};

template<>
struct adl_serializer<tpcc::OrderStatusOrderLine> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusOrderLine &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusOrderLine &v);
};

template<>
struct adl_serializer<tpcc::OrderStatusResult> {
  static void to_json(nlohmann::json &j, const tpcc::OrderStatusResult &v);
  static void from_json(const nlohmann::json &j, tpcc::OrderStatusResult &v);
};
}