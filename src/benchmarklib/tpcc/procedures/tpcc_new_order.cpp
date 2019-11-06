#include <ctime>
#include <random>

#include "tpcc/tpcc_random_generator.hpp"
#include "tpcc_new_order.hpp"

namespace opossum {

TPCCNewOrder::TPCCNewOrder(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  std::uniform_int_distribution<> warehouse_dist{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  std::uniform_int_distribution<> district_dist{1, 10};
  d_id = district_dist(_random_engine);

  c_id = static_cast<int>(_tpcc_random_generator.nurand(1023, 1, 3000));

  std::uniform_int_distribution<> num_items_dist{5, 15};
  ol_cnt = num_items_dist(_random_engine);

  order_lines.resize(ol_cnt);
  for (auto& order_line : order_lines) {
    order_line.ol_i_id = static_cast<int32_t>(_tpcc_random_generator.nurand(8191, 1, 100'000));

    std::uniform_int_distribution<> home_warehouse_dist{1, 100};
    // 1% chance of remote warehouses (if more than one).
    auto is_home_warehouse = num_warehouses == 1 || home_warehouse_dist(_random_engine) > 1;
    if (is_home_warehouse) {
      order_line.ol_supply_w_id = w_id;
    } else {
      // Choose a warehouse that is different from w_id
      do {
        order_line.ol_supply_w_id = warehouse_dist(_random_engine);
      } while (order_line.ol_supply_w_id == w_id);
    }

    std::uniform_int_distribution<> quantity_dist{1, 10};
    order_line.ol_quantity = quantity_dist(_random_engine);
  }

  std::uniform_int_distribution<> is_erroneous_dist{1, 100};
  // 1% chance of erroneous procedures.
  if (is_erroneous_dist(_random_engine) == 1) {
    order_lines.back().ol_i_id = UNUSED_ITEM_ID;  // A non-existing item ID
  }

  o_entry_d = static_cast<int32_t>(std::time(nullptr));
}

bool TPCCNewOrder::_on_execute() {
  // Retrieve W_TAX, the warehouse tax rate
  const auto warehouse_select_pair =
      _sql_executor.execute(std::string{"SELECT W_TAX FROM WAREHOUSE WHERE W_ID = "} + std::to_string(w_id));
  const auto& warehouse_table = warehouse_select_pair.second;
  Assert(warehouse_table && warehouse_table->row_count() == 1, "Did not find warehouse (or found more than one)");
  const auto w_tax = warehouse_table->get_value<float>(ColumnID{0}, 0);
  Assert(w_tax >= 0.f && w_tax <= .2f, "Invalid warehouse tax rate encountered");

  // Find the district tax rate and the next order ID
  const auto district_select_pair =
      _sql_executor.execute(std::string{"SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = "} +
                            std::to_string(w_id) + " AND D_ID = " + std::to_string(d_id));
  const auto& district_table = district_select_pair.second;
  Assert(district_table && district_table->row_count() == 1, "Did not find district (or found more than one)");
  const auto d_tax = district_table->get_value<float>(ColumnID{0}, 0);
  Assert(d_tax >= 0.f && d_tax <= .2f, "Invalid warehouse tax rate encountered");
  const auto d_next_o_id = district_table->get_value<int32_t>(ColumnID{1}, 0);
  o_id = d_next_o_id;

  // The TPC-C requires D_NEXT_O_ID to have a capacity of 10,000,000, so int is enough. For long runs, we still
  // might want to change this. Remember to touch all *_O_ID fields.
  Assert(d_next_o_id < std::numeric_limits<int>::max(), "Reached maximum for D_NEXT_O_ID, consider using LONG");
  // Update the next order ID (D_NEXT_O_ID). This is probably the biggest bottleneck as it leads to a high number of
  // MVCC conflicts.
  const auto district_update_pair =
      _sql_executor.execute(std::string{"UPDATE DISTRICT SET D_NEXT_O_ID = "} + std::to_string(d_next_o_id + 1) +
                            " WHERE D_W_ID = " + std::to_string(w_id) + " AND D_ID = " + std::to_string(d_id));
  if (district_update_pair.first != SQLPipelineStatus::Success) {
    return false;
  }

  // Find the customer with their discount rate, last name, and credit status
  const auto customer_select_pair = _sql_executor.execute(
      std::string{"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = "} + std::to_string(w_id) +
      " AND C_D_ID = " + std::to_string(d_id) + " AND C_ID = " + std::to_string(c_id));
  const auto& customer_table = customer_select_pair.second;
  Assert(customer_table && customer_table->row_count() == 1, "Did not find customer (or found more than one)");
  const auto c_discount = customer_table->get_value<float>(ColumnID{0}, 0);
  Assert(c_discount >= 0.f && c_discount <= .5f, "Invalid customer discount rate encountered");
  const auto c_last = customer_table->get_value<pmr_string>(ColumnID{1}, 0);
  const auto c_credit = customer_table->get_value<pmr_string>(ColumnID{2}, 0);
  Assert(c_credit == "GC" || c_credit == "BC", "Invalid customer credit encountered");

  // Check if all order lines are local
  auto o_all_local = true;
  for (const auto& order_line : order_lines) {
    // This is technically known when we create the procedure, but TPC-C wants us to calculate it live.
    if (order_line.ol_supply_w_id != w_id) o_all_local = false;
  }

  // Insert row into NEW_ORDER
  const auto new_order_insert_pair =
      _sql_executor.execute(std::string{"INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES ("} +
                            std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ")");
  Assert(new_order_insert_pair.first == SQLPipelineStatus::Success, "INSERT should not fail");

  // Insert row into ORDER
  // TODO(anyone): add NULL support to O_CARRIER_ID - also adapt check_consistency in tpcc_benchmark.cpp
  const auto order_insert_pair = _sql_executor.execute(
      std::string{"INSERT INTO \"ORDER\" (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, "
                  "O_OL_CNT, O_ALL_LOCAL) VALUES ("} +
      std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ", " + std::to_string(c_id) +
      ", " + std::to_string(o_entry_d) + ", -1, " + std::to_string(ol_cnt) + ", " + (o_all_local ? "1" : "0") + ")");
  Assert(order_insert_pair.first == SQLPipelineStatus::Success, "INSERT should not fail");

  // Iterate over order lines
  auto order_line_idx = size_t{0};
  for (const auto& order_line : order_lines) {
    ++order_line_idx;  // 1-indexed

    const auto item_select_pair =
        _sql_executor.execute(std::string{"SELECT I_ID, I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = "} +
                              std::to_string(order_line.ol_i_id));
    const auto& item_table = item_select_pair.second;
    if (item_table->row_count() == 0) {
      // A simulated error, roll back the transaction and return. These transactions are counted towards the number of
      // successful TPC-C procedures, so `true` is returned.
      _sql_executor.rollback();
      return true;
    }

    const auto i_price = item_table->get_value<float>(ColumnID{1}, 0);

    // Retrieve the STOCK entry. Currently, this is done in the loop and it should be more performant to do a similar
    // `IN (...)` optimization. Not sure how legal that is though.
    const auto stock_select_pair = _sql_executor.execute(
        std::string{"SELECT S_QUANTITY, S_DIST_"} + (d_id < 10 ? "0" : "") + std::to_string(d_id) +
        ", S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT FROM STOCK WHERE S_I_ID = " + std::to_string(order_line.ol_i_id) +
        " AND S_W_ID = " + std::to_string(order_line.ol_supply_w_id));
    const auto& stock_table = stock_select_pair.second;
    Assert(stock_table && stock_table->row_count() == 1, "Did not find stock entry (or found more than one)");
    const auto s_quantity = stock_table->get_value<int32_t>(ColumnID{0}, 0);
    const auto s_dist = stock_table->get_value<pmr_string>(ColumnID{1}, 0);
    const auto s_data = stock_table->get_value<pmr_string>(ColumnID{2}, 0);
    const auto s_ytd = stock_table->get_value<int32_t>(ColumnID{3}, 0);
    const auto s_order_cnt = stock_table->get_value<int32_t>(ColumnID{4}, 0);
    const auto s_remote_cnt = stock_table->get_value<int32_t>(ColumnID{4}, 0);

    // Calculate the new values for S_QUANTITY, S_YTD, S_ORDER_CNT
    auto new_s_quantity = 0;
    if (s_quantity >= order_line.ol_quantity + 10) {
      // Reduce the stock level appropriately if at least 10 items remain in stock
      new_s_quantity = s_quantity - order_line.ol_quantity;
    } else {
      new_s_quantity = s_quantity - order_line.ol_quantity + 91;
    }
    const auto new_s_ytd = s_ytd + order_line.ol_quantity;
    const auto new_s_order_cnt = s_order_cnt + 1;
    const auto new_s_remote_cnt = s_remote_cnt + (order_line.ol_supply_w_id == w_id ? 0 : 1);

    // Update the STOCK entry
    const auto stock_update_pair = _sql_executor.execute(
        std::string{"UPDATE STOCK SET S_QUANTITY = "} + std::to_string(new_s_quantity) +
        ", S_YTD = " + std::to_string(new_s_ytd) + ", S_ORDER_CNT = " + std::to_string(new_s_order_cnt) +
        ", S_REMOTE_CNT = " + std::to_string(new_s_remote_cnt) + " WHERE S_I_ID = " +
        std::to_string(order_line.ol_i_id) + " AND S_W_ID = " + std::to_string(order_line.ol_supply_w_id));
    if (stock_update_pair.first != SQLPipelineStatus::Success) {
      return false;
    }

    // Calculate price of line item (OL_AMOUNT)
    const auto ol_amount = order_line.ol_quantity * i_price;

    // Add to ORDER_LINE
    // TODO(anyone): This can be made faster if we interpret "For each O_OL_CNT item on the order" less strictly and
    //               allow for a single insert at the end
    // TODO(anyone): Use actual NULL for OL_DELIVERY_D
    const auto order_line_insert_pair = _sql_executor.execute(
        std::string{"INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, "
                    "OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES ("} +
        std::to_string(o_id) + ", " + std::to_string(d_id) + ", " + std::to_string(w_id) + ", " +
        std::to_string(order_line_idx) + ", " + std::to_string(order_line.ol_i_id) + ", " +
        std::to_string(order_line.ol_supply_w_id) + ", -1, " + std::to_string(order_line.ol_quantity) + ", " +
        std::to_string(ol_amount) + ", '" + std::string{s_dist} + "')");
    Assert(order_line_insert_pair.first == SQLPipelineStatus::Success, "INSERT should not fail");
  }

  _sql_executor.commit();
  return true;
}

}  // namespace opossum
