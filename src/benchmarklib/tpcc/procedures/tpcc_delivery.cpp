#include <ctime>
#include <random>

#include "tpcc_delivery.hpp"

#include "operators/print.hpp"

namespace opossum {

TpccDelivery::TpccDelivery(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  std::uniform_int_distribution<> carrier_dist{1, 10};
  _o_carrier_id = carrier_dist(_random_engine);

  _ol_delivery_d = std::time(nullptr);
}

void TpccDelivery::execute() {
  for (auto d_id = 1; d_id <= 10; ++d_id) {
    // This could be optimized by querying only once and grouping by NO_D_ID
    auto new_order_table = _execute_sql(std::string{"SELECT COUNT(*), MIN(NO_O_ID) FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(_w_id) + " AND NO_D_ID = " + std::to_string(d_id));

    // TODO this would not be necessary if get_value returned NULLs as nullopt
    if (new_order_table->get_value<int64_t>(ColumnID{0}, 0) == 0) continue;

    auto no_o_id = new_order_table->get_value<int32_t>(ColumnID{1}, 0);

    // Delete from NEW_ORDER
    _execute_sql(std::string{"DELETE FROM NEW_ORDER WHERE NO_W_ID = "} + std::to_string(_w_id) + " AND NO_D_ID = " + std::to_string(d_id) + " AND NO_O_ID = " + std::to_string(no_o_id));

    // Get customer ID
    auto order_table = _execute_sql(std::string{"SELECT O_C_ID FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(_w_id) + " AND O_D_ID = " + std::to_string(d_id) + " AND O_ID = " + std::to_string(no_o_id));
    auto o_c_id = order_table->get_value<int32_t>(ColumnID{0}, 0);

    // Update ORDER
    _execute_sql(std::string{"UPDATE \"ORDER\" SET O_CARRIER_ID = "} + std::to_string(_o_carrier_id) + " WHERE O_W_ID = " + std::to_string(_w_id) + " AND O_D_ID = " + std::to_string(d_id) + " AND O_ID = " + std::to_string(no_o_id));

    // Retrieve amount from ORDER_LINE
    auto order_line_table = _execute_sql(std::string{"SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = " + std::to_string(no_o_id));
    auto amount = order_line_table->get_value<double>(ColumnID{0}, 0);

    // Set delivery date in ORDER_LINE
    _execute_sql(std::string{"UPDATE ORDER_LINE SET OL_DELIVERY_D = "} + std::to_string(_ol_delivery_d) + " WHERE OL_W_ID = " + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = " + std::to_string(no_o_id));

    // Update balance and delivery count for customer
    _execute_sql(std::string{"UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + "} + std::to_string(amount) + ", C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE C_W_ID = " + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = " + std::to_string(o_c_id));
  }

  _transaction_context->commit();
}

std::ostream& TpccDelivery::print(std::ostream& stream) const {
  stream << "Delivery";
  return stream;
}

}
