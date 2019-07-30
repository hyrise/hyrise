#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TPCCStockLevel : public AbstractTPCCProcedure {
 public:
  TPCCStockLevel(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool execute() override;

 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;       // Home warehouse ID    [1..num_warehouses]
  int32_t _d_id;       // District ID          [1..10]
  int32_t _threshold;  // Minimum stock level  [10..20]
};

}  // namespace opossum
