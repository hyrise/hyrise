#include <memory>
#include <new>
#include <vector>

#include <random>
#include "benchmark/benchmark.h"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/zipfian_int_distribution.hpp"

namespace hyrise {

/**
 * Bechmark idea:
 * Sacle read ops with difedderne page size
 * Scale read ops with single page size and different DRAM size ratios
 * Use zipfian skews to test different hit and miss rates for single pahe size and diffeent dram size ratios
 * 
*/

class BufferManagerFixture : public benchmark::Fixture {
 public:
  constexpr static auto PAGE_SIZE_TYPE = MIN_PAGE_SIZE_TYPE;
  constexpr static auto DEFAULT_DRAM_BUFFER_POOL_SIZE = 1 << 30;  // 1 GB
  constexpr static auto CACHE_LINE_SIZE = 64;

  void SetUp(const ::benchmark::State& state) {
    if (state.thread_index() == 0) {
      _buffer_manager = BufferManager({.dram_buffer_pool_size = DEFAULT_DRAM_BUFFER_POOL_SIZE,
                                       .memory_node = NO_NUMA_MEMORY_NODE,
                                       .ssd_path = "/home/nriek/hyrise-fork/benchmarks"});
    }
  }

  void TearDown(const ::benchmark::State& state) {}

 protected:
  BufferManager _buffer_manager;
};

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerPinForWrite)(benchmark::State& state) {
  const auto DRAM_SIZE_RATIO = state.range(0);
  const auto MAX_PAGE_IDX = DRAM_SIZE_RATIO * (DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(PAGE_SIZE_TYPE));
  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < MAX_PAGE_IDX; ++idx) {
      auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, MAX_PAGE_IDX);

  for (auto _ : state) {
    auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_exclusive(page_id);
    state.PauseTiming();
    std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    state.ResumeTiming();
    _buffer_manager.unpin_exclusive(page_id);
    // benchmark::ClobberMemory();
  }

  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerPinForReadZipfian)(benchmark::State& state) {
  const auto DRAM_SIZE_RATIO = state.range(0);
  const auto MAX_PAGE_IDX = DRAM_SIZE_RATIO * (DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(PAGE_SIZE_TYPE));
  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < MAX_PAGE_IDX; ++idx) {
      auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  zipfian_int_distribution<> distr(0, MAX_PAGE_IDX);
  // Warmup
  for (auto i = 0; i < MAX_PAGE_IDX; ++i) {
    auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);
    _buffer_manager.unpin_shared(page_id);
  }

  for (auto _ : state) {
    auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);

    state.PauseTiming();
    auto page_ptr = _buffer_manager._get_page_ptr(page_id);
    for (auto i = 0; i < page_id.num_bytes(); i += CACHE_LINE_SIZE) {
      __builtin_prefetch(&page_ptr[i]);
      benchmark::DoNotOptimize(page_ptr[i]);
    }
    state.ResumeTiming();

    _buffer_manager.unpin_shared(page_id);
    benchmark::ClobberMemory();
  }

  auto metrics = _buffer_manager.metrics();
  state.counters["CacheHitRate"] = static_cast<double>(metrics->total_hits.load()) /
                                   static_cast<double>(metrics->total_hits.load() + metrics->total_misses.load());
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerPinForReadRandom)(benchmark::State& state) {
  const auto DRAM_SIZE_RATIO = state.range(0);
  const auto MAX_PAGE_IDX = DRAM_SIZE_RATIO * (DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(PAGE_SIZE_TYPE));
  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < MAX_PAGE_IDX; ++idx) {
      auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, MAX_PAGE_IDX);

  // Warmup
  for (auto i = 0; i < MAX_PAGE_IDX; ++i) {
    auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_exclusive(page_id);
    _buffer_manager.unpin_exclusive(page_id);
  }

  for (auto _ : state) {
    auto page_id = PageID{PAGE_SIZE_TYPE, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);

    state.PauseTiming();
    auto page_ptr = _buffer_manager._get_page_ptr(page_id);
    for (auto i = 0; i < page_id.num_bytes(); i += CACHE_LINE_SIZE) {
      __builtin_prefetch(&page_ptr[i]);
      benchmark::DoNotOptimize(page_ptr[i]);
    }
    state.ResumeTiming();

    _buffer_manager.unpin_shared(page_id);
    benchmark::ClobberMemory();
  }

  state.counters["CacheHitRate"] =
      _buffer_manager.metrics()->total_hits /
      static_cast<double>(_buffer_manager.metrics()->total_hits + _buffer_manager.metrics()->total_misses);
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesInMemory)(benchmark::State& state) {
  const auto page_size_type = static_cast<PageSizeType>(state.range(0));
  const auto max_page_idx = DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(page_size_type);

  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < max_page_idx; ++idx) {
      auto page_id = PageID{page_size_type, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, max_page_idx);

  for (auto _ : state) {
    auto page_id = PageID{page_size_type, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);

    state.PauseTiming();
    auto page_ptr = _buffer_manager._get_page_ptr(page_id);
    for (auto i = 0; i < page_id.num_bytes(); i += CACHE_LINE_SIZE) {
      __builtin_prefetch(&page_ptr[i]);
      benchmark::DoNotOptimize(page_ptr[i]);
    }
    state.ResumeTiming();

    _buffer_manager.unpin_shared(page_id);
    // benchmark::ClobberMemory();
  }
  state.SetLabel(std::string(magic_enum::enum_name(page_size_type)));
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesOutMemory)(benchmark::State& state) {
  const auto page_size_type = static_cast<PageSizeType>(state.range(0));
  constexpr auto out_memory_ratio = size_t{4};
  const auto max_page_idx = out_memory_ratio * DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(page_size_type);

  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < max_page_idx; ++idx) {
      auto page_id = PageID{page_size_type, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, max_page_idx);

  for (auto _ : state) {
    auto page_id = PageID{page_size_type, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);

    state.PauseTiming();
    auto page_ptr = _buffer_manager._get_page_ptr(page_id);
    for (auto i = 0; i < page_id.num_bytes(); i += CACHE_LINE_SIZE) {
      __builtin_prefetch(&page_ptr[i]);
      benchmark::DoNotOptimize(page_ptr[i]);
    }
    state.ResumeTiming();

    _buffer_manager.unpin_shared(page_id);
    // benchmark::ClobberMemory();
  }
  state.SetLabel(std::string(magic_enum::enum_name(page_size_type)));
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_DEFINE_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesOutMemoryWithRead)(benchmark::State& state) {
  const auto page_size_type = static_cast<PageSizeType>(state.range(0));
  constexpr auto out_memory_ratio = size_t{4};
  const auto max_page_idx = out_memory_ratio * DEFAULT_DRAM_BUFFER_POOL_SIZE / bytes_for_size_type(page_size_type);

  if (state.thread_index() == 0) {
    for (auto idx = size_t{0}; idx < max_page_idx; ++idx) {
      auto page_id = PageID{page_size_type, static_cast<size_t>(idx)};
      std::memset(_buffer_manager._get_page_ptr(page_id), 0x1337, page_id.num_bytes());
    }
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distr(0, max_page_idx);

  for (auto _ : state) {
    auto page_id = PageID{page_size_type, static_cast<size_t>(distr(gen))};
    _buffer_manager.pin_shared(page_id, AccessIntent::Read);

    auto page_ptr = _buffer_manager._get_page_ptr(page_id);
    for (auto i = 0; i < page_id.num_bytes(); i += CACHE_LINE_SIZE) {
      __builtin_prefetch(&page_ptr[i]);
      benchmark::DoNotOptimize(page_ptr[i]);
    }
    _buffer_manager.unpin_shared(page_id);
    // benchmark::ClobberMemory();
  }
  state.SetLabel(std::string(magic_enum::enum_name(page_size_type)));
  state.SetItemsProcessed(int64_t(state.iterations()));
  state.SetBytesProcessed(int64_t(state.iterations()) * bytes_for_size_type(PAGE_SIZE_TYPE));
}

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerPinForWrite)
    ->DenseRange(1, 4)
    ->ThreadRange(1, 128)
    ->UseRealTime();

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerPinForReadZipfian)
    ->DenseRange(1, 4)
    ->ThreadRange(1, 48)
    ->UseRealTime();

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerPinForReadRandom)
    ->DenseRange(1, 4)
    ->ThreadRange(1, 48)
    ->UseRealTime();

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesInMemory)
    ->DenseRange(static_cast<size_t>(MIN_PAGE_SIZE_TYPE), static_cast<size_t>(MAX_PAGE_SIZE_TYPE))
    ->ThreadRange(1, 48)
    ->UseRealTime();

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesOutMemory)
    ->DenseRange(static_cast<size_t>(MIN_PAGE_SIZE_TYPE), static_cast<size_t>(MAX_PAGE_SIZE_TYPE))
    ->ThreadRange(1, 48)
    ->UseRealTime();

BENCHMARK_REGISTER_F(BufferManagerFixture, BM_BufferManagerMultiplePageSizesOutMemoryWithRead)
    ->DenseRange(static_cast<size_t>(MIN_PAGE_SIZE_TYPE), static_cast<size_t>(MAX_PAGE_SIZE_TYPE))
    ->ThreadRange(1, 48)
    ->UseRealTime();

}  // namespace hyrise

// TODO: Vary single page size (8 and 256 KB), In-memory vs out-of-memory, include CXL, hyperthreading, and NUMA