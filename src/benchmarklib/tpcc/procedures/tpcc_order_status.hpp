#pragma once

#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

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
  // They are initialized with an invalid value.
  int32_t o_id{-1};                         // Order ID
  int32_t o_entry_d{-1};                    // Entry date of the order (created by new-order)
  std::optional<int32_t> o_carrier_id{-1};  // Carrier ID (created by new-order, may be NULL if undelivered)
  int32_t ol_quantity_sum{0};               // Sum of the quantities in the order lines, stored for verification

  // clang-format off
  static constexpr auto PREPARED_STATEMENTS = std::to_array({
      "PREPARE order_status_select_customer_by_id FROM 'SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE order_status_select_customer_by_name FROM 'SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST'",  // NOLINT(whitespace/line_length)
      "PREPARE order_status_retrieve_order FROM 'SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM \"ORDER\" WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC'",  // NOLINT(whitespace/line_length)
      "PREPARE order_status_retrieve_order_lines FROM 'SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?'",  // NOLINT(whitespace/line_length)
  });
  // clang-format on
};

}  // namespace hyrise
