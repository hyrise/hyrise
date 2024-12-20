#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "storage/buffer/volatile_region.hpp"
#include "types.hpp"

namespace hyrise {
template <auto MoveFunc, NodeID source_node, NodeID target_node>
void BM_VolatileRegionPageMovement(benchmark::State& state) {
  static constexpr uint64_t NUM_OPS = 10000;
  static constexpr auto size_type = PageSizeType::KiB256;
  static auto volatile_region = std::unique_ptr<VolatileRegion>();
  static auto mapped_region = std::unique_ptr<VolatileRegion::MappedPageRegion>();

  if (state.thread_index() == 0) {
    mapped_region = std::make_unique(VolatileRegion::create_mapped_region();
    volatile_region = std::make_unique<VolatileRegion>(size_type, mapped_region,
                                                       mapped_region + bytes_for_size_type(size_type) * NUM_OPS);
    for (auto page_index = uint64_t{0}; page_index < NUM_OPS; ++page_index) {
      volatile_region->mbind_to_numa_node(PageID{size_type, page_index}, source_node);
    }
    std::memset(mapped_region, 0x1, bytes_for_size_type(size_type) * NUM_OPS);
  }

  const auto num_pages_per_thread = NUM_OPS / state.threads();
  const auto start_page_idx = uint64_t{state.thread_index() * num_pages_per_thread};
  const auto end_page_idx = start_page_idx + num_pages_per_thread;

  auto latencies = uint64_t{0};

  for (auto _ : state) {
    for (auto page_index = start_page_idx; page_index < end_page_idx; ++page_index) {
      const auto page_id = PageID{size_type, page_index};
      std::invoke(MoveFunc, volatile_region.get(), page_id, target_node);
    }
  }

  state.SetItemsProcessed(num_pages_per_thread);
  state.SetBytesProcessed(num_pages_per_thread * bytes_for_size_type(size_type));

  if (state.thread_index() == 0) {
    VolatileRegion::unmap_region(mapped_region);
  }
}

BENCHMARK(BM_VolatileRegionPageMovement<&VolatileRegion::mbind_to_numa_node, NodeID{0}, NodeID{1}>)
    ->DenseThreadRange(1, 48, 1)
    ->Iterations(1)
    ->Name("VolatileRegion/mbind")
    ->UseRealTime();

BENCHMARK(BM_VolatileRegionPageMovement<&VolatileRegion::move_page_to_numa_node, NodeID{0}, NodeID{1}>)
    ->DenseThreadRange(1, 48, 1)
    ->Iterations(1)
    ->Name("VolatileRegion/move_pages")
    ->UseRealTime();

BENCHMARK(BM_VolatileRegionPageMovement<&VolatileRegion::memcpy_page_to_numa_node, NodeID{0}, NodeID{1}>)
    ->DenseThreadRange(1, 48, 1)
    ->Iterations(1)
    ->Name("VolatileRegion/memcpy")
    ->UseRealTime();
}  // namespace hyrise