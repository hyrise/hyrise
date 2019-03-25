#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TpccDelivery : public AbstractTpccProcedure {
public:
  TpccDelivery(const int num_warehouses);

  void execute() override;
  std::ostream& print(std::ostream& stream = std::cout) const override;

protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;           // Home warehouse ID    [1..num_warehouses]
  int64_t _o_carrier_id;   // Carrier ID           [1..10]
  int64_t _ol_delivery_d;  // Current datetime
};

}
