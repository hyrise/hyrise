#include <memory>

#include "benchmark/benchmark.h"

#include "benchmark_config.hpp"
#include "hyrise.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "tpcds/tpcds_table_generator.hpp"

namespace hyrise {

void silent_tpcds_table_generation(uint32_t scale_factor, std::shared_ptr<BenchmarkConfig> config) {
  auto* initial_buffer = std::cout.rdbuf();

  std::cout.rdbuf(nullptr);
  TPCDSTableGenerator(scale_factor, config).generate_and_store();
  std::cout.rdbuf(initial_buffer);
}

static void BM_ips4o(benchmark::State& state) {
  const auto node_queue_scheduler = std::make_shared<NodeQueueScheduler>();
  Hyrise::get().set_scheduler(node_queue_scheduler);

  auto& storage_manager = Hyrise::get().storage_manager;

  auto benchmark_config = std::make_shared<BenchmarkConfig>();
  benchmark_config->cache_binary_tables = true;
  benchmark_config->encoding_config = EncodingConfig{SegmentEncodingSpec{EncodingType::Unencoded}};

  silent_tpcds_table_generation(10, benchmark_config);
  auto cs_table = storage_manager.get_table("catalog_sales");

  auto cs_table_wrapper = std::make_shared<TableWrapper>(cs_table);
  cs_table_wrapper->never_clear_output();
  cs_table_wrapper->execute();

  auto sort_definitions = std::vector<SortColumnDefinition>{};
  const auto sort_columns =
      std::vector<std::string>{"cs_warehouse_sk", "cs_ship_mode_sk", "cs_promo_sk", "cs_quantity"};
  for (const auto& column_name : sort_columns) {
    sort_definitions.emplace_back(cs_table->column_id_by_name(column_name));
  }

  auto sort_config = Sort::Config();
  sort_config.block_size = state.range(0);
  sort_config.min_blocks_per_stripe = 32;
  sort_config.max_parallelism = 16;
  sort_config.bucket_count = state.range(1);
  sort_config.samples_per_classifier = state.range(2);

  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(cs_table_wrapper, sort_definitions, Chunk::DEFAULT_SIZE,
                                       hyrise::Sort::ForceMaterialization::No, sort_config);
    sort->execute();
  }

  node_queue_scheduler->finish();
}

BENCHMARK(BM_ips4o)->ArgsProduct({{4, 32, 64, 128, 256, 512}, {16, 32, 64}, {1, 2, 4, 8}});

}  // namespace hyrise
