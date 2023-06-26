#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

class PageMigrationFixture : public benchmark::Fixture {
 public:
  void SetUp(const ::benchmark::State& state) {
    _mapped_region = create_mapped_region();
  }

  void TearDown(const ::benchmark::State& state) {
    unmap_region(_mapped_region);
  }

 protected:
  std::byte* _mapped_region;
};

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemory)(benchmark::State& state) {
  auto size_type = static_cast<PageSizeType>(state.range(0));
  const auto num_bytes = bytes_for_size_type(size_type);
  constexpr auto VIRT_SIZE = 1UL * 1024 * 1024 * 1024;
  const auto times = VIRT_SIZE / num_bytes;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif

  for (auto _ : state) {
    state.PauseTiming();
#if HYRISE_NUMA_SUPPORT
    numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
#endif
    state.ResumeTiming();
    for (int idx = 0; idx < times; ++idx) {
#if HYRISE_NUMA_SUPPORT
      numa_tonode_memory(_mapped_region + idx * num_bytes, num_bytes, 2);
#endif
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()) * times);
  state.SetBytesProcessed(int64_t(state.iterations()) * times * num_bytes);
}

BENCHMARK_DEFINE_F(PageMigrationFixture, BM_ToNodeMemoryLatency)(benchmark::State& state) {
  auto size_type = static_cast<PageSizeType>(state.range(0));
  const auto num_bytes = bytes_for_size_type(size_type);
  constexpr auto VIRT_SIZE = 5UL * 1024 * 1024 * 1024;

#if HYRISE_NUMA_SUPPORT
  numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
  std::memset(_mapped_region, 0x1, VIRT_SIZE);
#endif

  auto i = 0;
  for (auto _ : state) {
#if HYRISE_NUMA_SUPPORT
    numa_tonode_memory(_mapped_region + (++i * num_bytes), num_bytes, 2);
#endif
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * num_bytes);
}

BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemory)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(MIN_PAGE_SIZE_TYPE),
                                               static_cast<u_int64_t>(MAX_PAGE_SIZE_TYPE), /*step=*/1)});
BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemoryLatency)
    ->ArgsProduct({benchmark::CreateDenseRange(static_cast<uint64_t>(MIN_PAGE_SIZE_TYPE),
                                               static_cast<u_int64_t>(MAX_PAGE_SIZE_TYPE), /*step=*/1)});
}  // namespace hyrise