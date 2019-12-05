#pragma once

#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

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
  int32_t o_id{-1};  // Order ID
};

}  // namespace opossum
