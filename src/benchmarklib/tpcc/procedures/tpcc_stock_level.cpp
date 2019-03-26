#include <ctime>
#include <random>

#include "tpcc_stock_level.hpp"

#include "operators/print.hpp"

namespace opossum {

TpccStockLevel::TpccStockLevel(const int num_warehouses) {
  // TODO this should be [1, n], but our data generator does [0, n-1]
  std::uniform_int_distribution<> warehouse_dist{0, num_warehouses - 1};
	_w_id = warehouse_dist(_random_engine);

  // TODO this should be [1, 10], but our data generator does [0, 9]
  std::uniform_int_distribution<> district_dist{0, 9};
  _d_id = district_dist(_random_engine);

  std::uniform_int_distribution<> threshold_dist{10, 20};
  _threshold = threshold_dist(_random_engine);
}

void TpccStockLevel::execute() {
  auto district_table = _execute_sql(std::string{"SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = "} + std::to_string(_w_id) + " AND D_ID = " + std::to_string(_d_id));
  Assert(district_table->row_count() == 1, "Did not find district (or found more than one)");
  auto first_o_id = district_table->get_value<int32_t>(ColumnID{0}, 0) - 20;

  auto order_line_table = _execute_sql(std::string{"SELECT OL_I_ID FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(_w_id) + " AND OL_D_ID = " + std::to_string(_d_id) + " AND OL_O_ID >= " + std::to_string(first_o_id));
  Assert(order_line_table->row_count() > 0, "Did not find latest orders");  // TODO - check if 20 should exist even at start time
  std::stringstream ol_i_ids_stream;
  const auto num_orders = static_cast<int>(order_line_table->row_count());
  for (auto order_line_idx = 0; order_line_idx < num_orders; ++order_line_idx) {
    ol_i_ids_stream << order_line_table->get_value<int32_t>(ColumnID{0}, order_line_idx) << ", ";
  }
  auto ol_i_ids = ol_i_ids_stream.str();
  ol_i_ids.resize(ol_i_ids.size() - 2);  // Remove final ", "

  _execute_sql(std::string{"SELECT COUNT(*) FROM STOCK WHERE S_I_ID IN ("} + ol_i_ids + ") AND S_W_ID = " + std::to_string(_w_id) + " AND S_QUANTITY < " + std::to_string(_threshold));

  // No need to commit the transaction as we have not modified anything
}

char TpccStockLevel::identifier() const { return 'S'; }

std::ostream& TpccStockLevel::print(std::ostream& stream) const {
  stream << "StockLevel";
  return stream;
}

}
