#pragma once

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

// Limitations:
// 2.7.2   The delivery procedure is not executed in any type of deferred mode (given than we do not have the
//         notion of terminals) and does not write a result file.
// 2.7.4.2 We do not report a skipped delivery ratio over 1%

class TPCCDelivery : public AbstractTPCCProcedure {
 public:
  TPCCDelivery(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool _on_execute() override;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;           // Home warehouse ID    [1..num_warehouses]
  int32_t o_carrier_id;   // Carrier ID           [1..10]
  int32_t ol_delivery_d;  // Current datetime
};

}  // namespace opossum
