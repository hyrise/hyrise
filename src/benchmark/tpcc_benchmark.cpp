#include <random>

#include "benchmark_runner.hpp"
#include "storage/storage_manager.hpp"
#include "tpcc/procedures/tpcc_delivery.hpp"
#include "tpcc/procedures/tpcc_new_order.hpp"
#include "tpcc/procedures/tpcc_order_status.hpp"
#include "tpcc/procedures/tpcc_payment.hpp"
#include "tpcc/procedures/tpcc_stock_level.hpp"
#include "tpcc/tpcc_table_generator.hpp"

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

  std::cout << "timestamp,procedure,success,duration" << std::endl;

  const auto benchmark_begin = std::chrono::high_resolution_clock::now();

  std::minstd_rand random_engine;
  std::uniform_int_distribution<> procedure_dist{0, 99};
  for (auto run = 0; run < 100000; ++run) {
    auto procedure = std::unique_ptr<AbstractTpccProcedure>{};

    auto procedure_random = procedure_dist(random_engine);

    BenchmarkSQLExecutor sql_executor{false, nullptr, std::nullopt};

    if (procedure_random < 4) {
      procedure = std::make_unique<TpccStockLevel>(num_warehouses, sql_executor);
    } else if (procedure_random < 8) {
      procedure = std::make_unique<TpccDelivery>(num_warehouses, sql_executor);
    } else if (procedure_random < 12) {
      procedure = std::make_unique<TpccOrderStatus>(num_warehouses, sql_executor);
    } else if (procedure_random < 55) {
      procedure = std::make_unique<TpccPayment>(num_warehouses, sql_executor);
    } else {
      procedure = std::make_unique<TpccNewOrder>(num_warehouses, sql_executor);
    }

    const auto procedure_begin = std::chrono::high_resolution_clock::now();
    const bool success = procedure->execute();
    const auto procedure_end = std::chrono::high_resolution_clock::now();
    std::cout << std::chrono::duration_cast<std::chrono::nanoseconds>(procedure_end - benchmark_begin).count() << ','
              << procedure->identifier() << ',' << (success ? '1' : '0') << ","
              << std::chrono::duration_cast<std::chrono::nanoseconds>(procedure_end - procedure_begin).count()
              << std::endl;
  }

  return 0;
}
