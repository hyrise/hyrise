#include <memory>
#include <vector>

#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <boost/container/vector.hpp>
#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/jemalloc_resource.hpp"
#include "storage/buffer/pin_guard.hpp"
#include "types.hpp"

namespace hyrise {

struct IntDefaultValue {
  static int get() {
    return 1337;
  }
};

struct ShortStringDefaultValue {
  static pmr_string get() {
    return pmr_string{"Hello"};
  }
};

struct LongStringDefaultValue {
  static pmr_string get() {
    return pmr_string{"Hello World! This is a long string that will be used as a default value for the vector."};
  }
};

template <typename Type, typename DefaultValue, auto MemoryResourceFunc>
static void BM_allocate_vector(benchmark::State& state) {
  using Allocator = PolymorphicAllocator<Type>;

  boost::container::pmr::memory_resource* memory_resource = MemoryResourceFunc();
  auto allocator = Allocator{memory_resource};
  auto pin_guard = AllocatorPinGuard{allocator};
  auto allocation_count = static_cast<size_t>(state.range(0));

  const auto default_value = DefaultValue::get();
  for (auto _ : state) {
    auto array = boost::container::vector<Type, Allocator>{allocation_count, default_value, allocator};
    auto size = array.size();
    benchmark::DoNotOptimize(size);
  }

  if (state.thread_index() == 0) {
    state.SetItemsProcessed(int64_t(state.iterations()) * state.threads());
    // TODO: String might have more
    if constexpr (std::is_same_v<Type, pmr_string>) {
      state.SetBytesProcessed(int64_t(state.iterations()) * allocation_count * sizeof(Type) * state.threads() *
                              default_value.capacity());
    } else {
      state.SetBytesProcessed(int64_t(state.iterations()) * allocation_count * sizeof(Type) * state.threads());
    }
  }
}

boost::container::pmr::memory_resource* get_buffer_manager_memory_resource() {
  return &JemallocMemoryResource::get();
}

// TODO: Dram vs cxl, int vs sting
BENCHMARK(BM_allocate_vector<int, IntDefaultValue, &get_default_jemalloc_memory_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/JemallocBufferManagerInt");
BENCHMARK(BM_allocate_vector<int, IntDefaultValue, &boost::container::pmr::new_delete_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/DefaultJemallocInt");

BENCHMARK(BM_allocate_vector<pmr_string, ShortStringDefaultValue, &get_default_jemalloc_memory_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/JemallocBufferManagerShortString");
BENCHMARK(BM_allocate_vector<pmr_string, ShortStringDefaultValue, &boost::container::pmr::new_delete_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/DefaultJemallocShortString");

BENCHMARK(BM_allocate_vector<pmr_string, LongStringDefaultValue, &get_default_jemalloc_memory_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/JemallocBufferManagerLongString");
BENCHMARK(BM_allocate_vector<pmr_string, LongStringDefaultValue, &boost::container::pmr::new_delete_resource>)
    ->Range(8, 1 << 15)
    ->DenseThreadRange(1, 48, 1)
    ->Name("BM_allocate_vector/DefaultJemallocLongString");

}  // namespace hyrise