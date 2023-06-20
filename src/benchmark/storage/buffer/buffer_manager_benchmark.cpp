// #include <memory>
// #include <vector>

// #include "benchmark/benchmark.h"
// #include "expression/expression_functional.hpp"
// #include "micro_benchmark_utils.hpp"
// #include "operators/table_scan.hpp"
// #include "operators/table_wrapper.hpp"
// #include "storage/buffer/buffer_manager.hpp"
// #include "storage/buffer/utils.hpp"
// #include "synthetic_table_generator.hpp"
// #include "types.hpp"

// using namespace hyrise::expression_functional;  // NOLINT

// namespace hyrise {

// static void BM_allocate_with_buffer_manager(benchmark::State& state, const BufferManagerMode mode,
//                                             const MigrationPolicy policy) {
//   const auto config = BufferManager::Config{
//       .dram_buffer_pool_size = static_cast<uint64_t>(state.range(0)),
//       .numa_buffer_pool_size = static_cast<uint64_t>(state.range(1)),
//       .migration_policy = policy,
//       .numa_memory_node = NO_NUMA_MEMORY_NODE,
//       .ssd_path = "./test.bin",
//       .enable_eviction_purge_worker = false,
//       .mode = mode,
//   };
//   Hyrise::get().buffer_manager = BufferManager{config};

//   auto benchmark_name = state.name();
//   std::replace(benchmark_name.begin(), benchmark_name.end(), '/', '-');

//   const auto sampler = std::make_shared<MetricsSampler>(
//       benchmark_name, std::filesystem::path("/tmp") / (benchmark_name + ".json"), &Hyrise::get().buffer_manager);

//   const auto chunk_size = ChunkOffset{2'000};
//   const auto row_count = size_t{500000};

//   // Setup synthetic data and scan
//   const auto table_generator = std::make_shared<SyntheticTableGenerator>();
//   const auto _table_wrapper_a =
//       std::make_shared<TableWrapper>(table_generator->generate_table(1ul, row_count, chunk_size));
//   _table_wrapper_a->never_clear_output();
//   _table_wrapper_a->execute();
//   const auto left_column_id = ColumnID{0};
//   const auto predicate_condition = PredicateCondition::GreaterThanEquals;
//   const auto right_operand = value_(7);
//   const auto left_operand =
//       pqp_column_(left_column_id, _table_wrapper_a->get_output()->column_data_type(left_column_id),
//                   _table_wrapper_a->get_output()->column_is_nullable(left_column_id), "");
//   const auto predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand);

//   auto warm_up = std::make_shared<TableScan>(_table_wrapper_a, predicate);
//   warm_up->execute();
//   for (auto _ : state) {
//     auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, predicate);
//     table_scan->execute();
//   }

//   state.counters["total_bytes_copied_from_ssd_to_dram"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_ssd_to_dram),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_ssd_to_numa"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_ssd_to_numa),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_numa_to_dram"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_numa_to_dram),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_dram_to_numa"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_dram_to_numa),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_dram_to_ssd"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_dram_to_ssd),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_numa_to_ssd"] = benchmark::Counter(
//       static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_numa_to_ssd),
//       benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);

//   state.counters["total_bytes_copied_to_ssd"] =
//       benchmark::Counter(static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_to_ssd),
//                          benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
//   state.counters["total_bytes_copied_from_ssd"] =
//       benchmark::Counter(static_cast<double>(Hyrise::get().buffer_manager.metrics()->total_bytes_copied_from_ssd),
//                          benchmark::Counter::Flags::kDefaults, benchmark::Counter::OneK::kIs1024);
// }

// BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, DramSSD, BufferManagerMode::DramSSD, EagerMigrationPolicy{})
//     ->RangeMultiplier(2)
//     ->Ranges({{2 << 18, 2 << 20}, {2 << 21, 2 << 21}})
//     ->Iterations(10);
// BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, DramNumaEmulationSSD, BufferManagerMode::DramNumaEmulationSSD,
//                   EagerMigrationPolicy{})
//     ->RangeMultiplier(2)
//     ->Ranges({{2 << 18, 2 << 20}, {2 << 21, 2 << 21}})
//     ->Iterations(10);
// // BENCHMARK_CAPTURE(BM_allocate_with_buffer_manager, DramNumaEmulationSSD - LazyMigrationPolicy,
// //                   BufferManagerMode::DramNumaEmulationSSD, LazyMigrationPolicy{})
// //     ->RangeMultiplier(2)
// //     ->Ranges({{2 << 18, 2 << 20}, {2 << 19, 2 << 21}})
// // ->Iterations(10);
// }  // namespace hyrise
