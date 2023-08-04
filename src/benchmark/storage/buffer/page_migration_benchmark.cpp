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
  const auto times = VIRT_SIZE / num_bytes;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif

  for (auto _ : state) {
    state.PauseTiming();
#if HYRISE_NUMA_SUPPORT
    explicit_move_pages(_mapped_region, VIRT_SIZE, 0);
#endif
    state.ResumeTiming();
    for (int idx = 0; idx < times; ++idx) {
#if HYRISE_NUMA_SUPPORT
      explicit_move_pages(_mapped_region + idx * num_bytes, num_bytes, target_node);
#endif
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()) * times);
  state.SetBytesProcessed(int64_t(state.iterations()) * times * num_bytes);
}

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemoryLatencyDramToCXL)(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 8UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  explicit_move_pages(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif
  // TODO: radnom
  auto i = 0;
  for (auto _ : state) {
#if HYRISE_NUMA_SUPPORT
    explicit_move_pages(_mapped_region + (++i * num_bytes), num_bytes, target_node);
#endif
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
}

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemoryLatencyCXLToDram)(benchmark::State& state) {
  const auto num_bytes = OS_PAGE_SIZE << static_cast<size_t>(state.range(0));
  constexpr auto VIRT_SIZE = 8UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, target_node);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif
  // TODO: radnom

  auto i = 0;
  for (auto _ : state) {
#if HYRISE_NUMA_SUPPORT
    explicit_move_pages(_mapped_region + (++i * num_bytes), num_bytes, 0);
#endif
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
}

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

  auto i = 0;
  for (auto _ : state) {
    for (std::size_t j = 0; j < pages.size(); ++j) {
      pages[j] = _mapped_region + i * num_bytes + j * OS_PAGE_SIZE;
      nodes[j] = target_node;
    }
#if HYRISE_NUMA_SUPPORT
    Assert(move_pages(0, pages.size(), pages.data(), nodes.data(), status.data(), MPOL_MF_MOVE) < 0,
           "Failed to move " + strerror(errno));
#endif
    i++;
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
}

BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemory)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->UseRealTime();
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemoryLatencyDramToCXL)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->UseRealTime();
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemoryLatencyCXLToDram)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->UseRealTime();
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_MovePagesLatency)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(0), static_cast<u_int64_t>(13), /*step=*/1)})
    ->UseRealTime();

}  // namespace hyrise