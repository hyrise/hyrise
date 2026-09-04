#pragma once

#include <array>

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

// Limitations:
// 2.7.2   The delivery procedure is not executed in any type of deferred mode (given than we do not have the
//         notion of terminals) and does not write a result file.
// 2.7.4.2 We do not report a skipped delivery ratio over 1%

class TPCCDelivery : public AbstractTPCCProcedure {
 public:
  TPCCDelivery(const int num_warehouses, BenchmarkSQLExecutor& init_sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;           // Home warehouse ID    [1..num_warehouses]
  int32_t o_carrier_id;   // Carrier ID           [1..10]
  int32_t ol_delivery_d;  // Current datetime

  // clang-format off
  static constexpr auto PREPARED_STATEMENTS = std::to_array({
      "PREPARE delivery_select_min_no_o_id FROM 'SELECT MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = ? AND NO_D_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_delete_new_order FROM 'DELETE FROM NEW_ORDER WHERE NO_W_ID = ? AND NO_D_ID = ? AND NO_O_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_select_customer_id FROM 'SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_update_order FROM 'UPDATE \"ORDER\" SET O_CARRIER_ID = ? WHERE O_W_ID = ? AND O_D_ID = ? AND O_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_select_sum_ol_amount FROM 'SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_update_order_line FROM 'UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE delivery_update_customer FROM 'UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?'",  // NOLINT(whitespace/line_length)
  });
  // clang-format on
};

}  // namespace hyrise
