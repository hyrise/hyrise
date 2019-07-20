#include <boost/container/small_vector.hpp>
#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TpccPayment : public AbstractTpccProcedure {
 public:
  TpccPayment(const int num_warehouses, BenchmarkSQLExecutor& sql_executor);

  [[nodiscard]] bool execute() override;
  char identifier() const override;

 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;    // Home warehouse ID       [1..num_warehouses]
  int32_t _d_id;    // District ID             [1..10]
  int32_t _c_w_id;  // Customer's warehouse    [1..num_warehouses]
  int32_t _c_d_id;  // Customer's district     [1..10]

  bool _select_customer_by_name;                // Whether the customer is identified by last name or ID
  std::variant<pmr_string, int32_t> _customer;  // Either a customer's ID or their last name (which is not unique)

  float _h_amount;  // The payment amount      [1..5000]
  int64_t _h_date;  // Current datetime
};

}  // namespace opossum
