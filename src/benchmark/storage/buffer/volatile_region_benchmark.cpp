#include <memory>

#include "benchmark/benchmark.h"
#include "micro_benchmark_utils.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/buffer/page.hpp"
#include "storage/buffer/volatile_region.hpp"

namespace hyrise {

static void BM_VolatileRegionAllocate(benchmark::State& state) {
  const auto num_frames = state.range(0);
  auto volatile_region = VolatileRegion(sizeof(Frame) * num_frames);

  for (auto _ : state) {
    benchmark::DoNotOptimize(volatile_region.allocate());
  }

  state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(num_frames));
}

BENCHMARK(BM_VolatileRegionAllocate)->Range(8, 8 << 10);

}  // namespace hyrise