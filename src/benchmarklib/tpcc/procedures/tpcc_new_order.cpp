#include <ctime>
#include <random>

#include "tpcc/tpcc_random_generator.hpp"
#include "tpcc_new_order.hpp"

namespace opossum {

TpccNewOrder::TpccNewOrder(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  // There are always exactly 9 districts per warehouse
  // TODO this should be [1, 10], but our data generator does [0, 9]
  std::uniform_int_distribution<> district_dist{0, 9};
	_d_id = district_dist(_random_engine);

  // Customer IDs are unique only per warehouse and district
  _c_id = static_cast<int>(_tpcc_random_generator.nurand(1023, 1, 3000));

  std::uniform_int_distribution<> num_items_dist{5, 15};
  _ol_cnt = num_items_dist(_random_engine);

  std::uniform_int_distribution<> is_erroneous_dist{1, 100};
  // If, from the range of 1 to 100, 17 is chosen, the order is erroneous. First number I came up with.
  _is_erroneous = is_erroneous_dist(_random_engine) == 17;

  _order_lines.resize(_ol_cnt);
  for (auto& order_line : _order_lines) {
    order_line.ol_i_id = static_cast<int32_t>(_tpcc_random_generator.nurand(8191, 1, 100000));

    // TODO Deal with single warehouse
    std::uniform_int_distribution<> home_warehouse_dist{1, 100};
    auto is_home_warehouse = home_warehouse_dist(_random_engine) != 17;
    if (is_home_warehouse) {
      order_line.ol_supply_w_id = _w_id;
    } else {
      // Choose a warehouse that is different from _w_id
      do {
        order_line.ol_supply_w_id = warehouse_dist(_random_engine);
      } while (order_line.ol_supply_w_id == _w_id);
    }

    std::uniform_int_distribution<> quantity_dist{1, 10};
    order_line.ol_quantity = quantity_dist(_random_engine);
  }

  _o_entry_d = std::time(nullptr);
}

void TpccNewOrder::execute() {
  // Retrieve W_TAX, the warehouse tax rate
  // TODO building the string this way is probably expensive. Not sure if this shows up for TPC-C.
  const auto warehouse_table = _execute_sql(std::string{"SELECT W_TAX FROM WAREHOUSE WHERE W_ID = "} + std::to_string(_w_id));
  DebugAssert(warehouse_table->row_count() == 1, "Did not find warehouse (or found more than one)");
  _w_tax = warehouse_table->get_value<float>(ColumnID{0}, 0);
  DebugAssert(_w_tax >= 0.f && _w_tax <= 2.f, "Invalid warehouse tax rate encountered");

  // Find the district tax rate and the next order ID
  const auto district_table = _execute_sql(std::string{"SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = "} + std::to_string(_w_id) + " AND D_ID = " + std::to_string(_d_id));
  DebugAssert(district_table->row_count() == 1, "Did not find district (or found more than one)");
  _d_tax = district_table->get_value<float>(ColumnID{0}, 0);
  _d_next_o_id = district_table->get_value<int32_t>(ColumnID{1}, 0);


  // Update the next order ID (D_NEXT_O_ID). This is probably the biggest bottleneck as it leads to a high number of
  // MVCC conflicts.
  _o_id = _d_next_o_id;
  _execute_sql(std::string{"UPDATE \"DISTRICT\" SET D_NEXT_O_ID = "} + std::to_string(++_d_next_o_id) + " WHERE D_W_ID = " + std::to_string(_w_id) + " AND D_ID = " + std::to_string(_d_id));

  // Find the customer with their discount rate, last name, and credit status
  const auto customer_table = _execute_sql(std::string{"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(_w_id) + " AND C_D_ID = " + std::to_string(_d_id) + " AND C_ID = " + std::to_string(_c_id));
  DebugAssert(customer_table->row_count() == 1, "Did not find customer (or found more than one)");
  _c_discount = customer_table->get_value<float>(ColumnID{0}, 0);
  _c_last = customer_table->get_value<pmr_string>(ColumnID{1}, 0);
  _c_credit = customer_table->get_value<pmr_string>(ColumnID{2}, 0);

  // Check if all order lines are local
  for (const auto& order_line : _order_lines) {
    // This is technically known when we create the procedure, but TPC-C wants us to calculate it live.
    if (order_line.ol_supply_w_id != _w_id) _o_all_local = false;
  }

  // Insert row into NEW_ORDER
  _execute_sql(std::string{"INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ("} + std::to_string(_o_id) + ", " + std::to_string(_d_id) + ", " + std::to_string(_w_id) + ")");

  // Insert row into ORDER
  // TODO add NULL support to O_CARRIER_ID
  _execute_sql(std::string{"INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES ("} + std::to_string(_o_id) + ", " + std::to_string(_d_id) + ", " + std::to_string(_w_id) + ", " + std::to_string(_c_id) + ", " + std::to_string(_o_entry_d) + ", -1, " + std::to_string(_ol_cnt) + ", " + (_o_all_local ? "1" : "0") + ")");

  // Iterate over order lines
  auto order_line_idx = size_t{0};
  for (const auto& order_line : _order_lines) {
    const auto item_table = _execute_sql(std::string{"SELECT I_ID, I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = "} + std::to_string(order_line.ol_i_id));
    if (item_table->row_count() == 0) {
      // A simulated error, roll back the transaction and return
      _transaction_context->rollback();
      return;
    }

    const auto i_price = item_table->get_value<float>(ColumnID{1}, 0);

    // Retrieve the STOCK entry. Currently, this is done in the loop and it should be more performant to do a similar `IN (...)` optimization. Not sure how legal that is though.
    // TODO Fix D_ID + 1
    const auto stock_table = _execute_sql(std::string{"SELECT S_QUANTITY, S_DIST_"} + (_d_id + 1 < 10 ? "0" : "") + std::to_string(_d_id + 1) + ", S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = " + std::to_string(order_line.ol_i_id) + " AND S_W_ID = " + std::to_string(order_line.ol_supply_w_id));
    DebugAssert(stock_table->row_count() == 1, "Did not find stock entry (or found more than one)");
    const auto s_quantity = stock_table->get_value<int32_t>(ColumnID{0}, 0);
    const auto s_dist = stock_table->get_value<pmr_string>(ColumnID{1}, 0);
    const auto s_data = stock_table->get_value<pmr_string>(ColumnID{2}, 0);
    const auto s_ytd = stock_table->get_value<int32_t>(ColumnID{3}, 0);
    const auto s_order_cnt = stock_table->get_value<int32_t>(ColumnID{4}, 0);
    const auto s_remote_cnt = stock_table->get_value<int32_t>(ColumnID{4}, 0);

    // Calculate the new values for S_QUANTITY, S_YTD, S_ORDER_CNT
    auto new_s_quantity = s_quantity;
    if (s_quantity >= order_line.ol_quantity + 10) {
      // Reduce the stock level appropriately if at least 10 items remain in stock
      new_s_quantity = s_quantity - order_line.ol_quantity;
    } else {
      new_s_quantity = s_quantity - order_line.ol_quantity + 91;
    }
    const auto new_s_ytd = s_ytd + order_line.ol_quantity;
    // TODO Is this increased even for remote orders?
    const auto new_s_order_cnt = s_order_cnt + 1;
    const auto new_s_remote_cnt = s_remote_cnt + (order_line.ol_supply_w_id == _w_id ? 0 : 1);

    // Update the STOCK entry
    _execute_sql(std::string{"UPDATE STOCK SET S_QUANTITY = "} + std::to_string(new_s_quantity) + ", S_YTD = " + std::to_string(new_s_ytd) + ", S_ORDER_CNT = " + std::to_string(new_s_order_cnt) + ", S_REMOTE_CNT = " + std::to_string(new_s_remote_cnt) + " WHERE S_I_ID = " + std::to_string(order_line.ol_i_id) + " AND S_W_ID = " + std::to_string(order_line.ol_supply_w_id));

    // Calculate price of line item (OL_AMOUNT)
    const auto ol_amount = order_line.ol_quantity * i_price;

    // Add to ORDER_LINE
    // TODO This can be made faster if we interpret "For each O_OL_CNT item on the order" less strictly and allow for a single insert at the end
    // TODO Use actual NULL for OL_DELIVERY_D
    _execute_sql(std::string{"INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES ("} + std::to_string(_o_id) + ", " + std::to_string(_d_id) + ", " + std::to_string(_w_id) + ", " + std::to_string(order_line_idx) + ", " + std::to_string(order_line.ol_i_id) + ", " + std::to_string(order_line.ol_supply_w_id) + ", -1, " + std::to_string(order_line.ol_quantity) + ", " + std::to_string(ol_amount) + ", '" + std::string{s_dist} + "')");
  }

  _transaction_context->commit();
}

std::ostream& TpccNewOrder::print(std::ostream& stream) const {
  stream << "NewOrder [W:" << _w_id << " D:" << _d_id << " C:" << _c_id << " OLs:" << _ol_cnt << " E:" << (_is_erroneous ? 1 : 0) << " || ";
  if (isnan(_w_tax)) {
    stream << "not executed yet";
  } else {
    stream << "W_TAX:" << _w_tax << " D_TAX:" << _d_tax << " D_NEXT_O_ID:" << _d_next_o_id << " C_DISCOUNT:" << _c_discount << " C_LAST:" << _c_last << " C_CREDIT:" << _c_credit;
  }
  stream << "]";
  return stream;
}

}
