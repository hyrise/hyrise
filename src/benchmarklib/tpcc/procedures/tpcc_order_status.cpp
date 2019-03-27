#include <ctime>
#include <random>

#include "tpcc_order_status.hpp"

namespace opossum {

TpccOrderStatus::TpccOrderStatus(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  // TODO this should be [1, 10], but our data generator does [0, 9]
  std::uniform_int_distribution<> district_dist{0, 9};
	_d_id = district_dist(_random_engine);

  // Select 6 out of 10 customers by last name
  std::uniform_int_distribution<> customer_selection_method_dist{1, 10};
  _select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (_select_customer_by_name) {
    _customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
    // TODO this should be [1, 3000], but our data generator does [0, 2999]
    _customer = _tpcc_random_generator.nurand(1023, 0, 2999);
  }
}

void TpccOrderStatus::execute() {
  auto customer_table = std::shared_ptr<const Table>{};
  auto customer_offset = size_t{};
  auto customer_id = int32_t{};

  if (!_select_customer_by_name) {
    // Case 1 - Select customer by ID
    customer_table = _execute_sql(std::string{"SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_d_id) + " AND C_ID = " + std::to_string(std::get<int32_t>(_customer)));
    Assert(customer_table->row_count() == 1, "Did not find customer by ID (or found more than one)");

    customer_offset = size_t{0};
    customer_id = std::get<int32_t>(_customer);
  } else {
    // Case 2 - Select customer by name
    customer_table = _execute_sql(std::string{"SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_d_id) + " AND C_LAST = '" + std::string{std::get<pmr_string>(_customer)} + "' ORDER BY C_FIRST");
    Assert(customer_table->row_count() >= 1, "Did not find customer by name");

    // Calculate ceil(n/2)
    customer_offset = static_cast<size_t>(std::min(std::ceil(customer_table->row_count() / 2.0), static_cast<double>(customer_table->row_count() - 1)));
    customer_id = customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);
  }

  // Retrieve order
  auto order_table = _execute_sql(std::string{"SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(_w_id) + " AND O_D_ID = " + std::to_string(_d_id) + " AND O_C_ID = " + std::to_string(customer_id) + " ORDER BY O_ID DESC");
  Assert(order_table->row_count() >= 1, "Did not find order");
  auto order_id = order_table->get_value<int32_t>(ColumnID{0}, 0);

  // Retrieve order lines
  auto order_line_table = _execute_sql(std::string{"SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(_d_id) + " AND OL_O_ID = " + std::to_string(order_id));

  // No need to commit the transaction as we have not modified anything
}

char TpccOrderStatus::identifier() const { return 'O'; }

std::ostream& TpccOrderStatus::print(std::ostream& stream) const {
  stream << "OrderStatus";
  return stream;
}

}
