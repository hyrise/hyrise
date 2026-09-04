#pragma once

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

class TPCCStockLevel : public AbstractTPCCProcedure {
 public:
  TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& init_sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // clang-format off
  static constexpr auto PREPARED_STATEMENTS = std::to_array({
    "PREPARE stock_level_next_o_id FROM 'SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?'",  // NOLINT(whitespace/line_length)
    // Some inputs are the same. We don't implement the postgresql syntax for prepared statement placeholders (e.g.
    // '$1' '$2' '$3') and use '?' instead. This hinders us from reusing inputs.
    "PREPARE stock_level_count_items FROM 'SELECT COUNT(DISTINCT(S_I_ID)) item_count FROM ORDER_LINE, STOCK WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID < ? AND OL_O_ID >= ? AND S_W_ID = ? AND S_I_ID = OL_I_ID AND S_QUANTITY < ?'"  // NOLINT(whitespace/line_length)
  });
  // clang-format on
 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;       // Home warehouse ID    [1..num_warehouses]
  int32_t _d_id;       // District ID          [1..10]
  int32_t _threshold;  // Minimum stock level  [10..20]
};

}  // namespace hyrise
