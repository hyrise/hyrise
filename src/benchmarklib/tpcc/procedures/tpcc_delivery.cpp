#include "tpcc_delivery.hpp"

#include <cstdint>
#include <ctime>
#include <random>
#include <string>

#include "benchmark_sql_executor.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "tpcc/procedures/abstract_tpcc_procedure.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

TPCCDelivery::TPCCDelivery(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor), ol_delivery_d{static_cast<int32_t>(std::time(nullptr))} {
  auto warehouse_dist = std::uniform_int_distribution<>{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  auto carrier_dist = std::uniform_int_distribution<>{1, 10};
  o_carrier_id = carrier_dist(_random_engine);
}

bool TPCCDelivery::_on_execute() {
  _sql_executor.execute(std::format(
      "PREPARE select_min_no_o_id (int) AS SELECT MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = {} AND NO_D_ID = ?",
      w_id));
  _sql_executor.execute(std::format(
      "PREPARE delete_new_order (int, int) AS DELETE FROM NEW_ORDER WHERE NO_W_ID = {} AND NO_D_ID = ? AND NO_O_ID = ?",
      w_id));
  _sql_executor.execute(std::format(
      "PREPARE select_o_c_id (int, int) AS SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = {} AND O_D_ID = ? AND O_ID = ?",
      w_id));
  _sql_executor.execute(
      std::format("PREPARE update_order (int, int) AS UPDATE \"ORDER\" SET O_CARRIER_ID = {} WHERE O_W_ID = {} AND "
                  "O_D_ID = ? AND O_ID = ?",
                  o_carrier_id, w_id));
  _sql_executor.execute(
      std::format("PREPARE select_sum_ol_amount (int, int) AS SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = {} "
                  "AND OL_D_ID = ? AND OL_O_ID = ?",
                  w_id));
  _sql_executor.execute(
      std::format("PREPARE update_order_line (int, int) AS UPDATE ORDER_LINE SET OL_DELIVERY_D = {} WHERE OL_W_ID = {} "
                  "AND OL_D_ID = ? AND OL_O_ID = ?",
                  ol_delivery_d, w_id));
  _sql_executor.execute(
      std::format("PREPARE update_customer (int, int, int) AS UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ?, "
                  "C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = {} AND C_D_ID = ? AND C_ID = ?",
                  w_id));

  for (auto d_id = 1; d_id <= 10; ++d_id) {
    // TODO(anyone): This could be optimized by querying only once and grouping by NO_D_ID
    const auto new_order_select_pair = _sql_executor.execute(std::format("EXECUTE select_min_no_o_id({})", d_id));
    const auto& new_order_table = new_order_select_pair.second;
    const auto min_no_o_id = new_order_table->get_value<int32_t>(ColumnID{0}, 0);
    if (!min_no_o_id) {
      continue;
    }

    // The oldest undelivered order in that district
    const auto no_o_id = *min_no_o_id;

    // Delete from NEW_ORDER
    const auto new_order_update_pair =
        _sql_executor.execute(std::format("EXECUTE delete_new_order({}, {})", d_id, no_o_id));
    if (new_order_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }

    // Get customer ID
    const auto order_select_pair = _sql_executor.execute(std::format("EXECUTE select_o_c_id({}, {})", d_id, no_o_id));
    const auto& order_table = order_select_pair.second;
    Assert(order_table && order_table->row_count() == 1, "Unexpected state of ORDER table.");
    const auto o_c_id = *order_table->get_value<int32_t>(ColumnID{0}, 0);

    // Update ORDER
    const auto order_update_pair = _sql_executor.execute(std::format("EXECUTE update_order({}, {})", d_id, no_o_id));
    if (order_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }

    // Retrieve amount from ORDER_LINE
    const auto order_line_select_pair =
        _sql_executor.execute(std::format("EXECUTE select_sum_ol_amount({}, {})", d_id, no_o_id));
    const auto& order_line_table = order_line_select_pair.second;
    Assert(order_line_table && order_line_table->row_count() == 1, "Unexpected state of ORDER_LINE table.");
    const auto amount = *order_line_table->get_value<double>(ColumnID{0}, 0);

    // Set delivery date in ORDER_LINE
    const auto order_line_update_pair =
        _sql_executor.execute(std::format("EXECUTE update_order_line({}, {})", d_id, no_o_id));
    Assert(order_update_pair.first == SQLPipelineStatus::Success,
           "We have already 'locked' the order ID when we deleted it from NEW_ORDER. No conflict should be possible.");

    // Update balance and delivery count for customer
    const auto customer_update_pair =
        _sql_executor.execute(std::format("EXECUTE update_customer({}, {}, {})", amount, d_id, o_c_id));
    if (customer_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }
  }

  // TPC-C would allow us to use one transaction per order. We did not yet measure if this would give us an advantage.
  _sql_executor.commit();
  return true;
}

}  // namespace hyrise
