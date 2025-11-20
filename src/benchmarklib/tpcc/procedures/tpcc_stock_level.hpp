#pragma once

#include "abstract_tpcc_procedure.hpp"

namespace hyrise {

class TPCCStockLevel : public AbstractTPCCProcedure {
 public:
  TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& init_sql_executor);

  [[nodiscard]] bool _on_execute() override;

 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;       // Home warehouse ID    [1..num_warehouses]
  int32_t _d_id;       // District ID          [1..10]
  int32_t _threshold;  // Minimum stock level  [10..20]
};

}  // namespace hyrise
