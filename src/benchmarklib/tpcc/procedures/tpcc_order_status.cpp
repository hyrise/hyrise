#include <ctime>
#include <random>

#include "tpcc_order_status.hpp"

namespace opossum {

TPCCOrderStatus::TPCCOrderStatus(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  std::uniform_int_distribution<> warehouse_dist{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  std::uniform_int_distribution<> district_dist{1, 10};
  d_id = district_dist(_random_engine);

  // Select 6 out of 10 customers by last name
  std::uniform_int_distribution<> customer_selection_method_dist{1, 10};
  select_customer_by_name = customer_selection_method_dist(_random_engine) <= 6;
  if (select_customer_by_name) {
    customer = pmr_string{_tpcc_random_generator.last_name(_tpcc_random_generator.nurand(255, 0, 999))};
  } else {
    customer = static_cast<int32_t>(_tpcc_random_generator.nurand(1023, 1, 3000));
  }
}

bool TPCCOrderStatus::_on_execute() {
  auto customer_table = std::shared_ptr<const Table>{};
  auto customer_id = int32_t{};

  if (!select_customer_by_name) {
    // Case 1 - Select customer by ID
    std::tie(std::ignore, customer_table) = _sql_executor.execute(
        std::string{"SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = "} +
        std::to_string(w_id) + " AND C_D_ID = " + std::to_string(d_id) +
        " AND C_ID = " + std::to_string(std::get<int32_t>(customer)));
    Assert(customer_table && customer_table->row_count() == 1, "Did not find customer by ID (or found more than one)");

    customer_id = std::get<int32_t>(customer);
  } else {
    // Case 2 - Select customer by name
    std::tie(std::ignore, customer_table) = _sql_executor.execute(
        std::string{"SELECT C_ID, C_BALANCE, C_FIRST, C_MIDDLE, C_LAST FROM CUSTOMER WHERE C_W_ID = "} +
        std::to_string(w_id) + " AND C_D_ID = " + std::to_string(d_id) + " AND C_LAST = '" +
        std::string{std::get<pmr_string>(customer)} + "' ORDER BY C_FIRST");
    Assert(customer_table->row_count() >= 1, "Did not find customer by name");

    // Calculate ceil(n/2)
    auto customer_offset = static_cast<size_t>(
        std::min(std::ceil(customer_table->row_count() / 2.0), static_cast<double>(customer_table->row_count() - 1)));
    customer_id = customer_table->get_value<int32_t>(ColumnID{0}, customer_offset);
  }

  // Retrieve order
  const auto order_select_pair = _sql_executor.execute(
      std::string{"SELECT O_ID, O_ENTRY_D, O_CARRIER_ID FROM \"ORDER\" WHERE O_W_ID = "} + std::to_string(w_id) +
      " AND O_D_ID = " + std::to_string(d_id) + " AND O_C_ID = " + std::to_string(customer_id) + " ORDER BY O_ID DESC");
  const auto& order_table = order_select_pair.second;
  // Returns multiple orders, we are interested in the latest one
  Assert(order_table && order_table->row_count() >= 1, "Did not find order");
  o_id = order_table->get_value<int32_t>(ColumnID{0}, 0);
  o_entry_d = order_table->get_value<int32_t>(ColumnID{1}, 0);
  o_carrier_id = order_table->get_value<int32_t>(ColumnID{2}, 0);

  // Retrieve order lines
  const auto order_line_select_pair = _sql_executor.execute(
      std::string{"SELECT OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT FROM ORDER_LINE WHERE OL_W_ID = "} +
      std::to_string(w_id) + " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID = " + std::to_string(o_id));
  const auto& order_line_table = order_line_select_pair.second;
  Assert(order_line_table && order_line_table->row_count() >= 5 && order_line_table->row_count() <= 15,
         "Did not find order lines");
  for (auto row = size_t{0}; row < order_line_table->row_count(); ++row) {
    ol_quantity_sum += order_line_table->get_value<int32_t>(ColumnID{2}, row);
  }

  _sql_executor.commit();
  return true;
}

}  // namespace opossum
