#pragma once

#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TPCCOrderStatus : public AbstractTPCCProcedure {
 public:
  TPCCOrderStatus(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;  // Home warehouse ID       [1..num_warehouses]
  int32_t d_id;  // District ID             [1..10]

  bool select_customer_by_name;                // Whether the customer is identified by last name or ID
  std::variant<pmr_string, int32_t> customer;  // Either a customer's ID or their last name (which is not unique)

  // Values calculated WHILE the procedure is executed, exposed for facilitating the tests:
  int32_t o_id{-1};            // Order ID
  int32_t o_entry_d{-1};       // Entry date of the order (created by new-order)
  int32_t o_carrier_id{-1};    // Carrier ID of the order (created by new-order)
  int32_t ol_quantity_sum{0};  // Sum of the quantities in the order lines, stored for verification
};

}  // namespace opossum
