#include <memory>
#include <vector>
#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#include <numaif.h>
#endif
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

// TODO memcpy

class PageMigrationFixture : public benchmark::Fixture {
 public:
  static constexpr size_t NUM_OPS = 10000;

  void SetUp(const ::benchmark::State& state) {
    _mapped_region = create_mapped_region();
  }

  void TearDown(const ::benchmark::State& state) {
    unmap_region(_mapped_region);
  }

 protected:
  NodeID target_node = NodeID{2};
  std::byte* _mapped_region;
};

// TODO: Preftech

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemory)(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 1UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif
  auto latencies = uint64_t{0};
  for (auto _ : state) {
    state.PauseTiming();
#if HYRISE_NUMA_SUPPORT
    explicit_move_pages(_mapped_region, VIRT_SIZE, 0);
#endif
    state.ResumeTiming();
    for (int idx = 0; idx < NUM_OPS; ++idx) {
#if HYRISE_NUMA_SUPPORT
      const auto timer_start = std::chrono::high_resolution_clock::now();

      explicit_move_pages(_mapped_region + idx * num_bytes, num_bytes, target_node);
      const auto timer_end = std::chrono::high_resolution_clock::now();
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
#endif
    }
    benchmark::ClobberMemory();
  }
  state.counters["latency_mean"] = benchmark::Counter(latencies / NUM_OPS);
  state.SetItemsProcessed(NUM_OPS);
  state.SetBytesProcessed(NUM_OPS * num_bytes);
}

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemoryLatencyDramToCXL)(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 8UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  explicit_move_pages(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif

  auto latencies = uint64_t{0};
  for (auto _ : state) {
#if HYRISE_NUMA_SUPPORT
    for (int i = 0; i < NUM_OPS; ++i) {
      const auto timer_start = std::chrono::high_resolution_clock::now();
      explicit_move_pages(_mapped_region + (++i * num_bytes), num_bytes, target_node);
      const auto timer_end = std::chrono::high_resolution_clock::now();
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
      latencies += latency;
    }
#endif
    benchmark::ClobberMemory();
  }
  state.counters["latency_mean"] = benchmark::Counter(latencies / NUM_OPS);
  state.counters["total_time"] = benchmark::Counter(latencies);
  state.SetItemsProcessed(NUM_OPS);
  state.SetBytesProcessed(NUM_OPS * num_bytes);
}

// BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemoryLatencyCXLToDram)(benchmark::State& state) {
//   const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
//   constexpr auto VIRT_SIZE = 8UL * 1024 * 1024 * 1024;

// #if HYRISE_NUMA_SUPPORT
//   numa_tonode_memory(_mapped_region, VIRT_SIZE, target_node);
//   std::memset(_mapped_region, 0x1, VIRT_SIZE);
// #endif
//   // TODO: radnom

//   auto i = 0;
//   auto latencies = uint64_t{0};
//   for (auto _ : state) {
// #if HYRISE_NUMA_SUPPORT
//     const auto timer_start = std::chrono::high_resolution_clock::now();

//     explicit_move_pages(_mapped_region + (++i * num_bytes), num_bytes, 0);
//     const auto timer_end = std::chrono::high_resolution_clock::now();
//     const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
//     latencies += latency;
// #endif
//   }
//   state.SetItemsProcessed(int64_t(state.iterations()));
//   state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
// }

// TODO: run with more time
BENCHMARK_DEFINE_F(PageMigrationFixture, BM_MovePagesLatency)(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 5UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif
  //__builtin_prefetch

  std::vector<void*> pages{};
  pages.resize(num_bytes / OS_PAGE_SIZE);
  std::vector<int> nodes{};
  nodes.resize(num_bytes / OS_PAGE_SIZE);
  std::fill(nodes.begin(), nodes.end(), target_node);
  std::vector<int> status{};
  status.resize(num_bytes / OS_PAGE_SIZE);

  auto latencies = uint64_t{0};
  for (auto _ : state) {
#if HYRISE_NUMA_SUPPORT
    for (int i = 0; i < NUM_OPS; ++i) {
      const auto timer_start = std::chrono::high_resolution_clock::now();
      for (std::size_t j = 0; j < pages.size(); ++j) {
        pages[j] = _mapped_region + i * num_bytes + j * OS_PAGE_SIZE;
        nodes[j] = target_node;
      }

      Assert(move_pages(0, pages.size(), pages.data(), nodes.data(), status.data(), MPOL_MF_MOVE) == 0,
             "Failed to move " + strerror(errno));
      const auto timer_end = std::chrono::high_resolution_clock::now();
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
      latencies += latency;
    }
#endif
  }
  state.counters["latency_mean"] = benchmark::Counter(latencies / NUM_OPS);
  state.counters["total_time"] = benchmark::Counter(latencies);
  state.SetItemsProcessed(NUM_OPS);
  state.SetBytesProcessed(NUM_OPS * num_bytes);
}

BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemory)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->Threads(1)
    ->Iterations(1)
    ->UseRealTime();
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemoryLatencyDramToCXL)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->Threads(1)
    ->Iterations(1)
    ->UseRealTime();
// BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemoryLatencyCXLToDram)
//     ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
//     ->Threads(1)
//     ->Iterations(1)
//     ->UseRealTime();
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_MovePagesLatency)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->Threads(1)
    ->Iterations(1)
    ->UseRealTime();

}  // namespace hyrise