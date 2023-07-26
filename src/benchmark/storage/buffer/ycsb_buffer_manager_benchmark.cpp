#include <memory>
#include <new>
#include <vector>

#include <random>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/jemalloc_resource.hpp"
#include "storage/buffer/zipfian_int_distribution.hpp"

namespace hyrise {

/**
 * Bechmark idea:
 * Sacle read ops with difedderne page size
 * Scale read ops with single page size and different DRAM size ratios
 * Use zipfian skews to test different hit and miss rates for single pahe size and diffeent dram size ratios
 * 
*/
template <YCSBTableAccessPattern AccessPattern, MigrationPolicy policy = LazyMigrationPolicy>
class YCSBBufferManagerFixture : public benchmark::Fixture {
 public:
  constexpr static auto PAGE_SIZE_TYPE = MIN_PAGE_SIZE_TYPE;
  constexpr static auto DEFAULT_DRAM_BUFFER_POOL_SIZE = 1UL * GB;
  constexpr static auto DEFAULT_NUMA_BUFFER_POOL_SIZE = 2UL * GB;

  constexpr static auto NUM_OPERATIONS = 1000000;

  YCSBTable table;
  YCSBOperations operations;
  boost::container::pmr::memory_resource* memory_resource;

  void SetUp(const ::benchmark::State& state) {
    if (state.thread_index() == 0) {
      auto config = BufferManager::Config::from_env();
      config.dram_buffer_pool_size = DEFAULT_DRAM_BUFFER_POOL_SIZE;
      config.numa_buffer_pool_size = DEFAULT_NUMA_BUFFER_POOL_SIZE;
      config.migration_policy = policy;

      Hyrise::get().buffer_manager = BufferManager(config);
#ifdef __APPLE__
      memory_resource = &Hyrise::get().buffer_manager;
#elif __linux__
      JemallocMemoryResource::get().reset();
      memory_resource = &JemallocMemoryResource::get();
#endif

      auto database_size = state.range(0) * GB;
      table = generate_ycsb_table(memory_resource, database_size);
      operations = generate_ycsb_operations<AccessPattern, NUM_OPERATIONS>(table.size(), 0.99);
      warmup(table, Hyrise::get().buffer_manager);
    }
  }
};

inline void run_ycsb(benchmark::State& state, YCSBTable& table, YCSBOperations& operations,
                     BufferManager& buffer_manager) {
  const auto operations_per_thread = operations.size() / state.threads();
  // TODO: Reset metrics of buffer manager
  auto bytes_processed = uint64_t{0};

  for (auto _ : state) {
    auto start = state.thread_index() * operations_per_thread;
    auto end = start + operations_per_thread;
    for (auto i = start; i < end; ++i) {
      const auto op = operations[i];
      auto start = std::chrono::high_resolution_clock::now();
      bytes_processed += execute_ycsb_action(table, buffer_manager, op);
      auto end = std::chrono::high_resolution_clock::now();
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }
  }
  /// TODO Combbe
  // state.counters["latency_mean"] = boost::accumulators::mean(latency_stats);
  // state.counters["latency_stddev"] = std::sqrt(boost::accumulators::variance(latency_stats));
  // state.counters["latency_median"] = boost::accumulators::median(latency_stats);
  // state.counters["latency_min"] = boost::accumulators::min(latency_stats);
  // state.counters["latency_max"] = boost::accumulators::max(latency_stats);
  // state.counters["latency_95percentile"] = boost::accumulators::quantile(latency_stats, 0.95);

  // TODO: not per thread?
  state.SetItemsProcessed(operations_per_thread);
  state.SetBytesProcessed(bytes_processed);
  state.counters["cache_hit_rate"] = buffer_manager.metrics()->hit_rate();
}

#define CONFIGURE_BENCHMARK(AccessPattern, Policy)                                       \
  BENCHMARK_TEMPLATE_DEFINE_F(YCSBBufferManagerFixture, BM_ycsb_##AccessPattern##Policy, \
                              YCSBTableAccessPattern::AccessPattern, Policy)             \
  (benchmark::State & state) {                                                           \
    run_ycsb(state, table, operations, Hyrise::get().buffer_manager);                    \
  }                                                                                      \
  BENCHMARK_REGISTER_F(YCSBBufferManagerFixture, BM_ycsb_##AccessPattern##Policy)        \
      ->ThreadRange(1, 48)                                                               \
      ->Iterations(1)                                                                    \
      ->Repetitions(1)                                                                   \
      ->UseRealTime()                                                                    \
      ->DenseRange(1, 4, 1)                                                              \
      ->Name("BM_ycsb/" #AccessPattern "/" #Policy);

CONFIGURE_BENCHMARK(ReadHeavy, LazyMigrationPolicy)
CONFIGURE_BENCHMARK(Balanced, LazyMigrationPolicy)
CONFIGURE_BENCHMARK(WriteHeavy, LazyMigrationPolicy)

CONFIGURE_BENCHMARK(ReadHeavy, EagerMigrationPolicy)
CONFIGURE_BENCHMARK(Balanced, EagerMigrationPolicy)
CONFIGURE_BENCHMARK(WriteHeavy, EagerMigrationPolicy)

CONFIGURE_BENCHMARK(ReadHeavy, DramOnlyMigrationPolicy)
CONFIGURE_BENCHMARK(Balanced, DramOnlyMigrationPolicy)
CONFIGURE_BENCHMARK(WriteHeavy, DramOnlyMigrationPolicy)

CONFIGURE_BENCHMARK(ReadHeavy, NumaOnlyMigrationPolicy)
CONFIGURE_BENCHMARK(Balanced, NumaOnlyMigrationPolicy)
CONFIGURE_BENCHMARK(WriteHeavy, NumaOnlyMigrationPolicy)

}  // namespace hyrise
