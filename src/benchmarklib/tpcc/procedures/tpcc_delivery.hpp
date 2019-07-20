#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

// Limitations:
// 2.7.2   The delivery procedure is not executed in any type of deferred mode (given than we do not have the
//         notion of terminals) and does not write a result file.
// 2.7.4.2 We do not report a skipped delivery ratio over 1%

class TpccDelivery : public AbstractTpccProcedure {
 public:
  TpccDelivery(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool execute() override;
  char identifier() const override;

  // Values generated BEFORE the procedure is executed:
  int32_t w_id;           // Home warehouse ID    [1..num_warehouses]
  int64_t o_carrier_id;   // Carrier ID           [1..10]
  int64_t ol_delivery_d;  // Current datetime
};

}  // namespace opossum
