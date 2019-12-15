#pragma once

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TPCCStockLevel : public AbstractTPCCProcedure {
 public:
  TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool _on_execute() override;

 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t w_id;       // Home warehouse ID    [1..num_warehouses]
  int32_t d_id;       // District ID          [1..10]
  int32_t threshold;  // Minimum stock level  [10..20]
};

}  // namespace opossum
