#include <boost/container/small_vector.hpp>

#include "abstract_tpcc_procedure.hpp"

namespace opossum {

class TpccNewOrder : public AbstractTpccProcedure {
 public:
  TpccNewOrder(const int num_warehouses, BenchmarkSQLExecutor sql_executor);

  [[nodiscard]] bool execute() override;
  char identifier() const override;

 protected:
  // Values generated BEFORE the procedure is executed:
  int32_t _w_id;  // Home warehouse ID             [1..num_warehouses]
  int32_t _d_id;  // District ID                   [1..10]
  int32_t _c_id;  // Customer ID                   [1..3000]

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

  int64_t _o_entry_d;  // Current datetime
};

}  // namespace opossum
