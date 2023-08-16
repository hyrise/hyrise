#include <ctime>
#include <random>

#include "tpcc_stock_level.hpp"

#include "operators/print.hpp"

namespace hyrise {

TPCCStockLevel::TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& sql_executor)
    : AbstractTPCCProcedure(sql_executor) {
  auto warehouse_dist = std::uniform_int_distribution<>{1, num_warehouses};
  w_id = warehouse_dist(_random_engine);

  auto district_dist = std::uniform_int_distribution<>{1, 10};
  d_id = district_dist(_random_engine);

  auto threshold_dist = std::uniform_int_distribution<>{10, 20};
  threshold = threshold_dist(_random_engine);
}

bool TPCCStockLevel::_on_execute() {
  // The implementation of this procedure follows the standard's example implementation (see Appendix A.5, p. 116) and
  // also conforms to py-tpcc (see https://github.com/apavlo/py-tpcc/blob/master/pytpcc/drivers/sqlitedriver.py).

  // Retrieve next order ID.
  const auto& [_, district_table] =
      _sql_executor.execute("SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = " + std::to_string(w_id) +
                            " AND D_ID = " + std::to_string(d_id));
  Assert(district_table && district_table->row_count() == 1, "Did not find district (or found more than one).");
  const auto next_o_id = *district_table->get_value<int32_t>(ColumnID{0}, 0);

  // Retrieve the number of items that have a stock level under the threshold value and belong to the 20 orders before
  // the next order ID. The alias is needed for verification with SQLite, which names the result column slightly
  // differently.
  _sql_executor.execute(
      "SELECT COUNT(DISTINCT(S_I_ID)) item_count FROM ORDER_LINE, STOCK WHERE OL_W_ID = " + std::to_string(w_id) +
      " AND OL_D_ID = " + std::to_string(d_id) + " AND OL_O_ID < " + std::to_string(next_o_id) +
      " AND OL_O_ID >= " + std::to_string(next_o_id - 20) + " AND S_W_ID = " + std::to_string(w_id) +
      " AND S_I_ID = OL_I_ID AND S_QUANTITY < " + std::to_string(threshold));

  _sql_executor.commit();
  return true;
}

}  // namespace hyrise
