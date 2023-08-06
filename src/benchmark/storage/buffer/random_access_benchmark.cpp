#include <fcntl.h>
#include <unistd.h>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <vector>
#include "benchmark/benchmark.h"
#include "buffer_benchmark_utils.hpp"

namespace hyrise {
enum AccessType { Read, TemporalWrite, NonTemporalWrite };

template <NodeID node, AccessType access>
void BM_RandomAccess(benchmark::State& state) {
  auto NUM_THREADS = state.threads();
  constexpr auto VIRT_SIZE = 2UL * GB;
  constexpr auto NUM_OPS_PER_THREAD = 10000;
  const auto SIZE_PER_THREAD = VIRT_SIZE / NUM_THREADS;

  static std::byte* mapped_region = nullptr;

  if (state.thread_index() == 0) {
    mapped_region = mmap_region(VIRT_SIZE);
    explicit_move_pages(mapped_region, VIRT_SIZE, node);
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  auto latencies = uint64_t{0};

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, SIZE_PER_THREAD / CACHE_LINE_SIZE - 1);

  auto start_addr = SIZE_PER_THREAD * state.thread_index();

  for (auto _ : state) {
    for (auto i = 0; i < NUM_OPS_PER_THREAD; ++i) {
      const auto addr = mapped_region + start_addr + distribution(gen) * CACHE_LINE_SIZE;
      DebugAssert(reinterpret_cast<uintptr_t>(addr) % CACHE_LINE_SIZE == 0, "Not cacheline aligned");
      const auto timer_start = std::chrono::high_resolution_clock::now();
      if constexpr (access == AccessType::Read) {
        simulate_cacheline_read(addr);
      } else if (access == AccessType::TemporalWrite) {
        simulate_cacheline_temporal_store(addr);
      } else if (access == AccessType::NonTemporalWrite) {
        simulate_cacheline_nontemporal_store(addr);
      } else {
        Fail("Unknown access type");
      }
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

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::Read>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/Read/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::TemporalWrite>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/TemporalWrite/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::NonTemporalWrite>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/NonTemporalWrite/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::Read>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/Read/CXL")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::TemporalWrite>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/TemporalWrite/CXL")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::NonTemporalWrite>)
    ->Threads(1)
    ->Iterations(1)
    ->Name("BM_RandomAccessLatency/NonTemporalWrite/CXL")
    ->UseRealTime();

}  // namespace hyrise