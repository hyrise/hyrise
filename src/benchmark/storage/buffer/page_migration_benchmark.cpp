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
  auto size_type = static_cast<PageSizeType>(state.range(1));
  const auto num_bytes = bytes_for_size_type(size_type);
  constexpr auto VIRT_SIZE = 0.5 * 1024 * 1024 * 1024;

  for (auto _ : state) {
    state.PauseTiming();
#if HYRISE_NUMA_SUPPORT
    numa_tonode_memory(_mapped_region, VIRT_SIZE, 0);
    for (int i = 0; i < VIRT_SIZE; i += OS_PAGE_SIZE) {
      auto page_start = _mapped_region + i;
      std::memset(page_start, 0x5, VIRT_SIZE);
    }
#endif
    state.ResumeTiming();
    for (int idx = 0; idx < state.range(1); ++idx) {
#if HYRISE_NUMA_SUPPORT
      numa_tonode_memory(_mapped_region + idx * num_bytes, num_bytes, 2);
#endif
    }
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(state.range(0)));
  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0)) * num_bytes);
}

BENCHMARK_REGISTER_F(PageMigrationFixture, BM_ToNodeMemory)
    ->ArgsProduct({benchmark::CreateRange(64, 2048, /*multi=*/2),
                   benchmark::CreateDenseRange(static_cast<uint64_t>(MIN_PAGE_SIZE_TYPE),
                                               static_cast<u_int64_t>(MAX_PAGE_SIZE_TYPE), /*step=*/1)});

}  // namespace hyrise