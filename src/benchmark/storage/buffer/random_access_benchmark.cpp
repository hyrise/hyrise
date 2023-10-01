#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <vector>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"

namespace hyrise {
enum AccessType { TemporalLoad, NonTemporalLoad, TemporalWrite, NonTemporalWrite };

template <NodeID node, AccessType access>
void BM_RandomAccess(benchmark::State& state) {
  auto NUM_THREADS = state.threads();
  constexpr auto VIRT_SIZE = 5UL * GB;
  constexpr auto NUM_OPS_PER_THREAD = 10000;
  const auto SIZE_PER_THREAD = (VIRT_SIZE / NUM_THREADS / CACHE_LINE_SIZE) * CACHE_LINE_SIZE;

  static std::byte* mapped_region = nullptr;

  if (state.thread_index() == 0) {
    mapped_region = mmap_region(VIRT_SIZE);
    explicit_move_pages(mapped_region, VIRT_SIZE, node);
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  auto latencies = uint64_t{0};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<uint64_t> distribution(0, SIZE_PER_THREAD / CACHE_LINE_SIZE - 1);

  auto start_addr = SIZE_PER_THREAD * state.thread_index();

  for (auto _ : state) {
    for (auto i = 0; i < NUM_OPS_PER_THREAD; ++i) {
      auto page_idx = distribution(gen);
      auto offset = page_idx * CACHE_LINE_SIZE;
      const auto addr = mapped_region + offset;
      DebugAssert(reinterpret_cast<uintptr_t>(addr) % CACHE_LINE_SIZE == 0, "Not cacheline aligned");
      DebugAssert(addr >= mapped_region, "not in region");
      DebugAssert(addr < mapped_region + VIRT_SIZE, "not in region");

      flush_cacheline(addr);
      flush_pipeline();
      const auto timer_start = std::chrono::high_resolution_clock::now();
      if constexpr (access == AccessType::TemporalLoad) {
        simulate_cacheline_load(addr);
      } else if (access == AccessType::NonTemporalLoad) {
        simulate_cacheline_nontemporal_load(addr);
      } else if (access == AccessType::TemporalWrite) {
        simulate_cacheline_temporal_store(addr);
      } else if (access == AccessType::NonTemporalWrite) {
        simulate_cacheline_nontemporal_store(addr);
      } else {
        Fail("Unknown access type");
      }
#ifdef __x86_64__
      _mm_mfence();
#endif
      const auto timer_end = std::chrono::high_resolution_clock::now();
      const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
      latencies += latency;
    }
  }

  state.counters["latency_mean"] = benchmark::Counter(latencies / NUM_OPS_PER_THREAD, benchmark::Counter::kAvgThreads);
  state.SetItemsProcessed(NUM_OPS_PER_THREAD);

  if (state.thread_index() == 0) {
    munmap_region(mapped_region, VIRT_SIZE);
  }
}

#define LATENCY_BM(node, label, access)                        \
  BENCHMARK(BM_RandomAccess<NodeID{node}, AccessType::access>) \
      ->Threads(1)                                             \
      ->Threads(2)                                             \
      ->Threads(4)                                             \
      ->Threads(8)                                             \
      ->Threads(16)                                            \
      ->Threads(32)                                            \
      ->Threads(48)                                            \
      ->Iterations(1)                                          \
      ->Name("BM_RandomAccessLatency/" #access "/" #label)     \
      ->UseRealTime();

LATENCY_BM(0, DRAM, TemporalLoad);
LATENCY_BM(0, DRAM, NonTemporalLoad);
LATENCY_BM(0, DRAM, TemporalWrite);
LATENCY_BM(0, DRAM, NonTemporalWrite);
LATENCY_BM(2, CXL, TemporalLoad);
LATENCY_BM(2, CXL, NonTemporalLoad);
LATENCY_BM(2, CXL, TemporalWrite);
LATENCY_BM(2, CXL, NonTemporalWrite);

}  // namespace hyrise