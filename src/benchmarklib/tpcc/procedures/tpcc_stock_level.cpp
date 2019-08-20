#include <ctime>
#include <random>

#include "tpcc_stock_level.hpp"

#include "operators/print.hpp"

namespace opossum {

TPCCStockLevel::TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  std::uniform_int_distribution<> warehouse_dist{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  std::uniform_int_distribution<> district_dist{1, 10};
  d_id = district_dist(_random_engine);

  std::uniform_int_distribution<> threshold_dist{10, 20};
  threshold = threshold_dist(_random_engine);
}

bool TPCCStockLevel::_on_execute() {
  // Retrieve next order ID
  const auto district_table_pair =
      _sql_executor.execute(std::string{"SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = "} + std::to_string(w_id) +
                            " AND D_ID = " + std::to_string(d_id));
  const auto& district_table = district_table_pair.second;
  Assert(district_table && district_table->row_count() == 1, "Did not find district (or found more than one)");

  // We check the stock levels for the 20 orders before that next order ID
  auto first_o_id = district_table->get_value<int32_t>(ColumnID{0}, 0) - 20;

  // Retrieve the distict item ids of those orders
  const auto order_line_table_pair = _sql_executor.execute(
      std::string{"SELECT DISTINCT OL_I_ID FROM ORDER_LINE WHERE OL_W_ID = "} + std::to_string(w_id) +
      " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID >= " + std::to_string(first_o_id));
  const auto& order_line_table = order_line_table_pair.second;

  // Build a string for the IN expression
  std::stringstream ol_i_ids_stream;
  for (auto order_line_idx = size_t{0}; order_line_idx < order_line_table->row_count(); ++order_line_idx) {
    ol_i_ids_stream << order_line_table->get_value<int32_t>(ColumnID{0}, order_line_idx) << ", ";
  }
  auto ol_i_ids = ol_i_ids_stream.str();
  ol_i_ids.resize(ol_i_ids.size() - 2);  // Remove final ", "

  // Retrieve the number of items that have a stock level under the threshold value
  _sql_executor.execute(std::string{"SELECT COUNT(*) FROM STOCK WHERE S_I_ID IN ("} + ol_i_ids +
                        ") AND S_W_ID = " + std::to_string(w_id) + " AND S_QUANTITY < " + std::to_string(threshold));

  _sql_executor.commit();
  return true;
}

}  // namespace opossum
