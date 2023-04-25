#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "micro_benchmark_utils.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/utils.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

static void BM_allocate_with_buffer_manager(benchmark::State& state, const BufferManagerMode mode) {
  const auto config = BufferManager::Config{
      .dram_buffer_pool_size = 5000000,    // 512 mb
      .numa_buffer_pool_size = 1UL << 34,  // 16 GB
      .migration_policy = EagerMigrationPolicy{},
      .numa_memory_node = NO_NUMA_MEMORY_NODE,
      .ssd_path = "./test.bin",
      .enable_eviction_worker = false,
      .mode = mode,
  };
  Hyrise::get().buffer_manager = BufferManager{config};

  const auto benchmark_name = state.name();
  const auto sampler = std::make_shared<MetricsSampler>(
      benchmark_name, std::filesystem::path("/tmp") / (benchmark_name + ".json"), &Hyrise::get().buffer_manager);

  const auto chunk_size = ChunkOffset{2'000};
  const auto row_count = size_t{500000};

  // Setup synthetic data and scan
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();
  const auto _table_wrapper_a =
      std::make_shared<TableWrapper>(table_generator->generate_table(1ul, row_count, chunk_size));
  _table_wrapper_a->never_clear_output();
  _table_wrapper_a->execute();
  const auto left_column_id = ColumnID{0};
  const auto predicate_condition = PredicateCondition::GreaterThanEquals;
  const auto right_operand = value_(7);
  const auto left_operand =
      pqp_column_(left_column_id, _table_wrapper_a->get_output()->column_data_type(left_column_id),
                  _table_wrapper_a->get_output()->column_is_nullable(left_column_id), "");
  const auto predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand);

  auto warm_up = std::make_shared<TableScan>(_table_wrapper_a, predicate);
  warm_up->execute();
  for (auto _ : state) {
    auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, predicate);
    table_scan->execute();
  }
}

// BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, DramSSD, BufferManagerMode::DramSSD)->Iterations(10);
// BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, NumaSSD, BufferManagerMode::NumaSSD)->Iterations(10);
// BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, NumaSSD, BufferManagerMode::DramNumaSSD)->Iterations(10);
BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, NumaSSD, BufferManagerMode::DramNumaEmulationSSD)->Iterations(10);

}  // namespace hyrise
