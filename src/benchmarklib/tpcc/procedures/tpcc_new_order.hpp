#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TpccNewOrder : public AbstractTpccProcedure {
public:
  TpccNewOrder(const int num_warehouses);

  void execute() override;
  char identifier() const override;
  std::ostream& print(std::ostream& stream = std::cout) const override;

protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;       // Home warehouse ID             [1..num_warehouses]
  int32_t _d_id;       // District ID                   [1..10]
  int32_t _c_id;       // Customer ID                   [1..2999]

  int32_t _ol_cnt;     // Number of items in the order  [5..15] - this is equal to _order_lines.size(), but we keep it
                       // as it is referenced frequently in the TPC-C
  bool _is_erroneous;  // 1 in 100 orders are simulated to be user-errors

  struct OrderLine {
  	int32_t ol_i_id;         // Item number             [1..100000] or 888888 for erroneous entries (only for last line)
  	int32_t ol_supply_w_id;  // Supplying warehouse     [1..num_warehouses], equal to _w_id in 99% of the cases
  	int32_t ol_quantity;     // Quantity                [1..10]
  };

  // Stores a maximum of 15 items (see _ol_cnt). Not using an array because we want size().
  boost::container::small_vector<OrderLine, 15> _order_lines;

  int64_t _o_entry_d;        // Current datetime

  // Values calculated DURING the execution of the procedure:
  float _w_tax = NAN;          // Warehouse's tax rate
  float _d_tax = NAN;          // District's tax rate
  int32_t _d_next_o_id = -1;   // District's next order ID
  int32_t _o_id;               // Order ID
  float _c_discount = NAN;     // Customer's discount rate
  pmr_string _c_last;          // Customer's last name
  pmr_string _c_credit;        // Customer's credit status
  bool _o_all_local = true;    // Is for every order lines ol_supply_w_id == _w_id?

};

}
