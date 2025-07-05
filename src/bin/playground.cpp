#include <random>

// This playground only compiles on Linux as we require Linux's perf and perfetto.
#include "benchmark_config.hpp"
#include "encoding_config.hpp"
#include "hyrise.hpp"
#include "operators/sort.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/encoding_type.hpp"
#include "tpcds/tpcds_table_generator.hpp"

using namespace hyrise;  // NOLINT(build/namespaces)

int main() {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto& storage_manager = Hyrise::get().storage_manager;

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}};

  auto* initial_buffer = std::cout.rdbuf();
  std::cout.rdbuf(nullptr);
  TPCDSTableGenerator(100, benchmark_config).generate_and_store();
  std::cout.rdbuf(initial_buffer);

  auto cs_table = storage_manager.get_table("catalog_sales");
  auto sort_definitions = std::vector<SortColumnDefinition>{};
  const auto sort_columns =
      std::vector<std::string>{"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk", "cs_quantity"};
  for (const auto& column_name : sort_columns) {
    sort_definitions.emplace_back(cs_table->column_id_by_name(column_name));
  }

  std::cout << "Size of table to sort: " << cs_table->row_count()
            << ". Memory Usage: " << cs_table->memory_usage(MemoryUsageCalculationMode::Sampled) << ".\n";

  perfetto_run(cs_table, sort_definitions);

  node_queue_scheduler->finish();

  return 0;
}
