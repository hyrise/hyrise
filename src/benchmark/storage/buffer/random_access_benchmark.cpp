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
  constexpr auto VIRT_SIZE = 2UL * GB;
  auto max_index = VIRT_SIZE / CACHE_LINE_SIZE - 1;

  static std::byte* mapped_region = nullptr;

  if (state.thread_index() == 0) {
    explicit_move_pages(mapped_region, VIRT_SIZE, node);
    std::memset(mapped_region, 0x1, VIRT_SIZE);
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> distribution(0, max_index);

  auto latencies = uint64_t{0};

  for (auto _ : state) {
    state.PauseTiming();
    auto curr_idx = distribution(gen);
    state.ResumeTiming();

    const auto timer_start = std::chrono::high_resolution_clock::now();
    if constexpr (access == AccessType::Read) {
      simulate_read(mapped_region + curr_idx, VIRT_SIZE);
    } else if (access == AccessType::TemporalWrite) {
      simulate_cacheline_temporal_store(mapped_region + curr_idx);
    } else if (access == AccessType::NonTemporalWrite) {
      simulate_cacheline_nontemporal_store(mapped_region + curr_idx);
    } else {
      Fail("Unknown access type");
    }
    const auto timer_end = std::chrono::high_resolution_clock::now();
    const auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(timer_end - timer_start).count();
    latencies += latency;
  }

  if (state.thread_index() == 0) {
    munmap_region(mapped_region, VIRT_SIZE);
    state.SetItemsProcessed(int64_t(state.iterations()) * state.threads());
  }
}

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::Read>)->Threads(1)->Name("BM_RandomRead/DRAM")->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::TemporalWrite>)
    ->Threads(1)
    ->Name("BM_RandomTemporalWrite/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{0}, AccessType::NonTemporalWrite>)
    ->Threads(1)
    ->Name("BM_RandomNonTemporalWrite/DRAM")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::Read>)->Threads(1)->Name("BM_RandomRead/CXL")->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::TemporalWrite>)
    ->Threads(1)
    ->Name("BM_RandomTemporalWrite/CXL")
    ->UseRealTime();

BENCHMARK(BM_RandomAccess<NodeID{2}, AccessType::NonTemporalWrite>)
    ->Threads(1)
    ->Name("BM_RandomNonTemporalWrite/CXL")
    ->UseRealTime();

}  // namespace hyrise