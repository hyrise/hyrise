#include <memory>

#include <boost/align/aligned_allocator.hpp>
#include "benchmark/benchmark.h"
#include "hyrise.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "storage/value_segment.hpp"
#include "utils/meta_tables/meta_system_utilization_table.hpp"

namespace hyrise {

static void BM_VolatileRegionAllocator(benchmark::State& state) {
  const auto num_pages = state.range(0);

  auto volatile_region = std::make_unique<VolatileRegion>(PageSizeType::KiB32, PageType::Dram,
                                                          num_pages * bytes_for_size_type(PageSizeType::KiB32));

  for (auto _ : state) {
    for (auto i = size_t{0}; i < num_pages; i++) {
      auto frame = volatile_region->allocate();
      benchmark::DoNotOptimize(frame);
    }
    state.PauseTiming();
    volatile_region = std::make_unique<VolatileRegion>(PageSizeType::KiB32, PageType::Dram,
                                                       num_pages * bytes_for_size_type(PageSizeType::KiB32));
    state.ResumeTiming();
  }

  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(num_pages) * bytes_for_size_type(PageSizeType::KiB32));
  state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(num_pages));
  state.SetLabel("Allocations with VolatileRegion (Buffer Pool)");
}

static void BM_VolatileRegionAllocatorCompareToBoostAlignedAllocator(benchmark::State& state) {
  const auto num_pages = state.range(0);
  using Array = std::array<char, bytes_for_size_type(PageSizeType::KiB32)>;
  auto allocator = boost::alignment::aligned_allocator<Array>{};

  auto allocations = std::vector<Array*>(num_pages);

  for (auto _ : state) {
    for (auto i = size_t{0}; i < num_pages; i++) {
      auto page = allocator.allocate(1);
      allocations[i] = page;
    }

    // Deallocate to avoid leaks
    benchmark::DoNotOptimize(allocations);
    state.PauseTiming();
    for (auto page : allocations) {
      allocator.deallocate(page, 0);
    }
    state.ResumeTiming();
  }

  state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(num_pages) * bytes_for_size_type(PageSizeType::KiB32));
  state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(num_pages));
  state.SetLabel("Allocations with Boost Aligned Allocator (malloc)");
}

BENCHMARK(BM_VolatileRegionAllocator)->RangeMultiplier(2)->Range(8, 8 << 5);
BENCHMARK(BM_VolatileRegionAllocatorCompareToBoostAlignedAllocator)->RangeMultiplier(2)->Range(8, 8 << 5);

}  // namespace hyrise