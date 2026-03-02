#pragma once

#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

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
  int32_t c_id{-1};  // Customer ID, initialized with invalid value

  // clang-format off
  static constexpr auto PREPARED_STATEMENTS = std::to_array({
    "PREPARE payment_select_warehouse (int) AS SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_YTD FROM WAREHOUSE WHERE W_ID = ?",
    "PREPARE payment_update_warehouse_ytd (float, int) AS UPDATE WAREHOUSE SET W_YTD = ? WHERE W_ID = ?",
    "PREPARE payment_select_district (int, int) AS SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_YTD FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?",
    "PREPARE payment_update_district_ytd (float, int, int) AS UPDATE DISTRICT SET D_YTD = ? WHERE D_W_ID = ? AND D_ID = ?",
    "PREPARE payment_select_customer_by_id (int, int, int) AS SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
    "PREPARE payment_select_customer_by_name (int, int, string) AS SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST",
    // The first two floats should be the same value. We don't implement the postgresql syntax for prepared statement
    // placeholders (e.g. '$1' '$2' '$3') and use '?' instead. This hinders us from reusing inputs.
    "PREPARE payment_update_customer_balance (float, float, int, int, int) AS UPDATE CUSTOMER SET C_BALANCE = C_BALANCE - ?, C_YTD_PAYMENT = C_YTD_PAYMENT + ?, C_PAYMENT_CNT = C_PAYMENT_CNT + 1 WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
    "PREPARE payment_update_customer_c_data (string, int, int, int) AS UPDATE CUSTOMER SET C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?",
    "PREPARE payment_insert_history (int, int, int, int, int, string, int, float) AS INSERT INTO HISTORY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID, H_DATA, H_DATE, H_AMOUNT) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
  });
  // clang-format on
};

}  // namespace hyrise
