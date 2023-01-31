#include <memory>

#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include "benchmark/benchmark.h"
#include "storage/buffer/page.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

// static void BM_VolatileRegionAllocateAndDeallocateSegmentInPage(benchmark::State& state) {
//   const auto num_frames = state.range(0);
//   auto volatile_region = VolatileRegion(sizeof(Frame));  // TODO: Increase the size
//   auto value_segment = std::make_shared<ValueSegment<int32_t>>(pmr_vector<int32_t>{17, 0, 1, 1},
//                                                                pmr_vector<bool>{true, true, false, false});

//   for (auto _ : state) {
//     auto frame = volatile_region.allocate();
//     boost::container::pmr::monotonic_buffer_resource resource{frame->data.data.data(), frame->data.data.size(),
//                                                               nullptr};
//     const auto allocator = PolymorphicAllocator<size_t>(&resource);
//     auto segment_in_page = value_segment->copy_using_allocator(allocator);
//     segment_in_page = nullptr;
//     volatile_region.deallocate(frame);
//   }

//   state.SetItemsProcessed(int64_t(state.iterations()) * int64_t(num_frames));
// }

// BENCHMARK(BM_VolatileRegionAllocateAndDeallocateSegmentInPage)->Range(8, 8 << 10);

}  // namespace hyrise