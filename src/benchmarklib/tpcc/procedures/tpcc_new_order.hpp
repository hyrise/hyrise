#pragma once

#include <array>

#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

class TPCCNewOrder : public AbstractTPCCProcedure {
 public:
  TPCCNewOrder(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // Used to simulate user data entry errors (compare 2.4.1.4). A number that is outside of the range of valid item ids
  // [1..100000] and is easy to recognize.
  static constexpr int UNUSED_ITEM_ID = 888'888;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;  // Home warehouse ID             [1..num_warehouses]
  int32_t d_id;  // District ID                   [1..10]
  int32_t c_id;  // Customer ID                   [1..3000]

  int32_t ol_cnt;  // Number of items in the order  [5..15] - this is equal to _order_lines.size(), but we keep it

  // as it is referenced frequently in the TPC-C

  struct OrderLine {
    int32_t ol_i_id;         // Item number             [1..100000] or 888888 for erroneous entries (only for last line)
    int32_t ol_supply_w_id;  // Supplying warehouse     [1..num_warehouses], equal to w_id in 99% of the cases
    int32_t ol_quantity;     // Quantity                [1..10]
  };

  // Stores a maximum of 15 items (see _ol_cnt). Not using an array because we want size().
  boost::container::small_vector<OrderLine, 15> order_lines;

  int32_t o_entry_d;  // Current datetime

  // Values calculated WHILE the procedure is executed, exposed for facilitating the tests:
  int32_t o_id{-1};  // Order ID, initialized with invalid value

  // clang-format off
  // NOLINTBEGIN(whitespace/line_length)
  static constexpr auto PREPARED_STATEMENTS = std::to_array({
      "PREPARE new_order_select_warehouse_tax FROM 'SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_district_next_order FROM 'SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_update_district FROM 'UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_W_ID = ? AND D_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_customer FROM 'SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?'",  // NOLINT(whitespace/line_length)
      // Our parser currently does not support parameters ('?') in the VALUES field of an INSERT statement. As these
      // statements would not be optimized much anyway, we do not use prepared statements for them.
      // "PREPARE new_order_insert_new_order FROM 'INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)'",
      // "PREPARE new_order_insert_order FROM 'INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, NULL, ?, ?)'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_item FROM 'SELECT I_ID, I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_01 FROM 'SELECT S_QUANTITY, S_DIST_01, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_02 FROM 'SELECT S_QUANTITY, S_DIST_02, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_03 FROM 'SELECT S_QUANTITY, S_DIST_03, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_04 FROM 'SELECT S_QUANTITY, S_DIST_04, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_05 FROM 'SELECT S_QUANTITY, S_DIST_05, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_06 FROM 'SELECT S_QUANTITY, S_DIST_06, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_07 FROM 'SELECT S_QUANTITY, S_DIST_07, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_08 FROM 'SELECT S_QUANTITY, S_DIST_08, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_09 FROM 'SELECT S_QUANTITY, S_DIST_09, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_select_stock_with_dist_10 FROM 'SELECT S_QUANTITY, S_DIST_10, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      "PREPARE new_order_update_stock FROM 'UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?'",  // NOLINT(whitespace/line_length)
      // "PREPARE new_order_insert_order_line FROM 'INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)'",  // NOLINT(whitespace/line_length)
  });
  // NOLINTEND(whitespace/line_length)
  // clang-format on
};

}  // namespace hyrise
