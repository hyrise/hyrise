#include "benchmark_runner.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/procedures/tpcc_delivery.hpp"
#include "tpcc/procedures/tpcc_new_order.hpp"
#include "tpcc/procedures/tpcc_order_status.hpp"
#include "tpcc/procedures/tpcc_payment.hpp"
#include "tpcc/procedures/tpcc_stock_level.hpp"
#include "tpcc/tpcc_table_generator.hpp"
#include "utils/timer.hpp"

// ...
// We do not execute the TPC-C benchmark through the BenchmarkRunner as the runner is focused on single queries,
// expressed as a single SQL statement. The transaction-based logic of the TPC-C leads to multiple integration
// problems, not only during execution, but also w.r.t. reporting and verification. Additionally, TPC-C requires
// us to execute a weighted mix of transactions, which is not needed for other supported benchmarks.
// We completely ignore the virtual terminals from the TPC-C specification (as does pretty much everyone else).

int main(int argc, char* argv[]) {
  using namespace opossum;

  auto cli_options = BenchmarkRunner::get_basic_cli_options("TPCC Benchmark");

  const auto num_warehouses = 2;

  auto table_generator = TpccTableGenerator{Chunk::DEFAULT_SIZE, num_warehouses};

  for (const auto& [name, table] : table_generator.generate_all_tables()) {
    StorageManager::get().add_table(name, table);
  }

  // {
  //   Timer t{};
  //   for (auto i = 0; i < 1000; ++i) {
  //     auto new_order_test = TpccNewOrder{num_warehouses};
  //     new_order_test.execute();
  //     std::cout << new_order_test << std::endl;
  //   }
  //   std::cout << t.lap_formatted() << std::endl;
  // }

  // {
  //   Timer t{};
  //   for (auto i = 0; i < 1000; ++i) {
  //     auto payment_test = TpccPayment{num_warehouses};
  //     payment_test.execute();
  //     std::cout << payment_test << std::endl;
  //   }
  //   std::cout << t.lap_formatted() << std::endl;
  // }

  // {
  //   Timer t{};
  //   for (auto i = 0; i < 1000; ++i) {
  //     auto order_status_test = TpccOrderStatus{num_warehouses};
  //     order_status_test.execute();
  //     std::cout << order_status_test << std::endl;
  //   }
  //   std::cout << t.lap_formatted() << std::endl;
  // }

  // {
  //   Timer t{};
  //   for (auto i = 0; i < 1000; ++i) {
  //     auto delivery_test = TpccDelivery{num_warehouses};
  //     delivery_test.execute();
  //     std::cout << delivery_test << std::endl;
  //   }
  //   std::cout << t.lap_formatted() << std::endl;
  // }

  {
    Timer t{};
    for (auto i = 0; i < 1000; ++i) {
      auto stock_level_test = TpccStockLevel{num_warehouses};
      stock_level_test.execute();
      std::cout << stock_level_test << std::endl;
    }
    std::cout << t.lap_formatted() << std::endl;
  }

  return 0;
}
