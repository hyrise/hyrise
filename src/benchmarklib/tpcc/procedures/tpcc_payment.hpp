#pragma once

#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TPCCPayment : public AbstractTPCCProcedure {
 public:
  TPCCPayment(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;    // Home warehouse ID       [1..num_warehouses]
  int32_t d_id;    // District ID             [1..10]
  int32_t c_w_id;  // Customer's warehouse    [1..num_warehouses]
  int32_t c_d_id;  // Customer's district     [1..10]

  bool select_customer_by_name;                // Whether the customer is identified by last name or ID
  std::variant<pmr_string, int32_t> customer;  // Either a customer's ID or their last name (which is not unique)

  float h_amount;  // The payment amount      [1..5000]
  int32_t h_date;  // Current datetime

  // Values calculated WHILE the procedure is executed, exposed for facilitating the tests:
  int32_t c_id{-1};
};

}  // namespace opossum
