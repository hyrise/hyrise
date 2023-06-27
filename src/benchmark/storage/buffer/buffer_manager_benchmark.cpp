#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

class BufferManagerFixture : public benchmark::Fixture {
 public:
  constexpr static auto DEFAULT_DRAM_BUFFER_POOL_SIZE = 1 << 30;  // 1 GB

  void SetUp(const ::benchmark::State& state) {
    if (state.thread_index() == 0) {
      _buffer_manager = BufferManager({
          .dram_buffer_pool_size = DEFAULT_DRAM_BUFFER_POOL_SIZE,
          .memory_node = NO_NUMA_MEMORY_NODE,
      });
    }
  }

  void TearDown(const ::benchmark::State& state) {}

 protected:
  BufferManager _buffer_manager;
};

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerPinForWrite)(benchmark::State& state) {
  constexpr auto PAGE_SIZE_TYPE = MIN_PAGE_SIZE_TYPE;

  auto page_ids = std::vector<PageID>{DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(PAGE_SIZE_TYPE)};
  auto i = size_t{0};
  for (auto& page_id : page_ids) {
    page_id = PageID{PAGE_SIZE_TYPE, i++};
  }

  for (auto _ : state) {
    auto page_id = INVALID_PAGE_ID;
    _buffer_manager.pin_for_write(page_id);
    state.PauseTiming();
    std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    state.ResumeTiming();
    _buffer_manager.unpin_for_write(page_id);
    benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerPinForWrite)->ThreadRange(1, 128)->UseRealTime();

}  // namespace hyrise

// TODO: Vary single page size (8 and 256 KB), In-memory vs out-of-memory, include CXL, hyperthreading, and NUMA