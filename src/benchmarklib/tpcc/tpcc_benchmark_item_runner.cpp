#include "tpcc_benchmark_item_runner.hpp"
#include "tpcc/procedures/tpcc_delivery.hpp"
#include "tpcc/procedures/tpcc_new_order.hpp"
#include "tpcc/procedures/tpcc_order_status.hpp"
#include "tpcc/procedures/tpcc_payment.hpp"
#include "tpcc/procedures/tpcc_stock_level.hpp"

namespace opossum {

TPCCBenchmarkItemRunner::TPCCBenchmarkItemRunner(const std::shared_ptr<BenchmarkConfig>& config, int num_warehouses)
    : AbstractBenchmarkItemRunner(config), _num_warehouses(num_warehouses) {}

const std::vector<BenchmarkItemID>& TPCCBenchmarkItemRunner::items() const {
  static const std::vector<BenchmarkItemID> items{BenchmarkItemID{0}, BenchmarkItemID{1}, BenchmarkItemID{2},
                                                  BenchmarkItemID{3}, BenchmarkItemID{4}};
  return items;
}

bool TPCCBenchmarkItemRunner::_on_execute_item(const BenchmarkItemID item_id, BenchmarkSQLExecutor& sql_executor) {
  switch (item_id) {
    // TODO Do something with failed transactions. If clients == 1 or scheduler off, assert that we have no aborts
    // TODO Also in the plot script
    case 0:
      return TPCCDelivery{_num_warehouses, sql_executor}.execute();
    case 1:
      return TPCCNewOrder{_num_warehouses, sql_executor}.execute();
    case 2:
      return TPCCOrderStatus{_num_warehouses, sql_executor}.execute();
    case 3:
      return TPCCPayment{_num_warehouses, sql_executor}.execute();
    case 4:
      return TPCCStockLevel{_num_warehouses, sql_executor}.execute();
    default:
      Fail("Invalid item_id");
  }
}

std::string TPCCBenchmarkItemRunner::item_name(const BenchmarkItemID item_id) const {
  // This should be the only place where this is needed. If we need an ID-to-name mapping (or vice versa) again, we
  // should use a proper bimap.
  switch (item_id) {
    case 0:
      return "Delivery";
    case 1:
      return "New-Order";
    case 2:
      return "Order-Status";
    case 3:
      return "Payment";
    case 4:
      return "Stock-Level";
    default:
      Fail("Invalid item_id");
  }
}

const std::vector<int>& TPCCBenchmarkItemRunner::weights() const {
  // Except for New-Order, the given weights are minimum (see 5.2.3 in the standard). Since New-Order is the transactino
  // being counted, we want it to have the maximum weight possible.
  static const std::vector<int> weights{4, 45, 4, 43, 4};
  return weights;
}

}  // namespace opossum
