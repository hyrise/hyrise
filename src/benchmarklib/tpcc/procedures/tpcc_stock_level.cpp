#include "tpcc_stock_level.hpp"

#include <cstdint>
#include <random>
#include <string>

#include "benchmark_sql_executor.hpp"
#include "tpcc/procedures/abstract_tpcc_procedure.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

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
      _sql_executor.execute(std::format("EXECUTE stock_level_next_o_id({}, {})", w_id, d_id));
  Assert(district_table && district_table->row_count() == 1, "Did not find district (or found more than one).");
  const auto next_o_id = *district_table->get_value<int32_t>(ColumnID{0}, 0);

  // Retrieve the number of items that have a stock level under the threshold value and belong to the 20 orders before
  // the next order ID. The alias is needed for verification with SQLite, which names the result column slightly
  // differently.
  _sql_executor.execute(std::format("EXECUTE stock_level_count_items({0}, {1}, {2}, {3}, {0}, {4})", w_id, d_id,
                                    next_o_id, next_o_id - 20, threshold));
  _sql_executor.commit();
  return true;
}

}  // namespace hyrise
