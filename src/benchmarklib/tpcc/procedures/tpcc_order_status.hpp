#include <boost/container/small_vector.hpp>
#include <variant>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TpccOrderStatus : public AbstractTpccProcedure {
public:
  TpccOrderStatus(const int num_warehouses);

  void execute() override;
  char identifier() const override;
  std::ostream& print(std::ostream& stream = std::cout) const override;

protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;       // Home warehouse ID       [1..num_warehouses]
  int32_t _d_id;       // District ID             [1..10]

  bool _select_customer_by_name;                // Whether the customer is identified by last name or ID
  std::variant<pmr_string, int32_t> _customer;  // Either a customer's ID or their last name (which is not unique)
};

}
