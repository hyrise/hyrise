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

  for (auto d_id = 1; d_id <= 10; ++d_id) {
    // TODO(anyone): This could be optimized by querying only once and grouping by NO_D_ID
    const auto new_order_select_pair =
        _sql_executor.execute(std::format("EXECUTE select_min_no_o_id({}, {})", w_id, d_id));
    const auto& new_order_table = new_order_select_pair.second;
    const auto min_no_o_id = new_order_table->get_value<int32_t>(ColumnID{0}, 0);
    if (!min_no_o_id) {
      continue;
    }

    // The oldest undelivered order in that district
    const auto no_o_id = *min_no_o_id;

    // Delete from NEW_ORDER
    const auto new_order_update_pair =
        _sql_executor.execute(std::format("EXECUTE delete_new_order({}, {}, {})", w_id, d_id, no_o_id));
    if (new_order_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }

    // Get customer ID
    const auto order_select_pair =
        _sql_executor.execute(std::format("EXECUTE select_o_c_id({}, {}, {})", w_id, d_id, no_o_id));
    const auto& order_table = order_select_pair.second;
    Assert(order_table && order_table->row_count() == 1, "Unexpected state of ORDER table.");
    const auto o_c_id = *order_table->get_value<int32_t>(ColumnID{0}, 0);

    // Update ORDER
    const auto order_update_pair =
        _sql_executor.execute(std::format("EXECUTE update_order({}, {}, {}, {})", o_carrier_id, w_id, d_id, no_o_id));
    if (order_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }

    // Retrieve amount from ORDER_LINE
    const auto order_line_select_pair =
        _sql_executor.execute(std::format("EXECUTE select_sum_ol_amount({}, {}, {})", w_id, d_id, no_o_id));
    const auto& order_line_table = order_line_select_pair.second;
    Assert(order_line_table && order_line_table->row_count() == 1, "Unexpected state of ORDER_LINE table.");
    const auto amount = *order_line_table->get_value<double>(ColumnID{0}, 0);

    // Set delivery date in ORDER_LINE
    const auto order_line_update_pair = _sql_executor.execute(
        std::format("EXECUTE update_order_line({}, {}, {}, {})", ol_delivery_d, w_id, d_id, no_o_id));
    Assert(order_update_pair.first == SQLPipelineStatus::Success,
           "We have already 'locked' the order ID when we deleted it from NEW_ORDER. No conflict should be possible.");

    // Update balance and delivery count for customer
    const auto customer_update_pair =
        _sql_executor.execute(std::format("EXECUTE update_customer({}, {}, {}, {})", amount, w_id, d_id, o_c_id));
    if (customer_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }
  }

  // TPC-C would allow us to use one transaction per order. We did not yet measure if this would give us an advantage.
  _sql_executor.commit();
  return true;
}

}  // namespace hyrise
