#include <memory>

#include "benchmark/benchmark.h"

#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "utils/scheduling_utils.hpp"

namespace hyrise {

static void BM_ChunkGroupingForScheduling(benchmark::State& state) {
  const auto use_scheduler = static_cast<bool>(state.range(0));

  if (use_scheduler) {
    Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  }

  constexpr auto CHUNK_COUNT = size_t{100'000};
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  const auto table = table_generator->generate_table(1, CHUNK_COUNT * 2, ChunkOffset{2});

  auto sum = size_t{0};
  for (auto _ : state) {
    auto batching_result = batch_chunks_for_scheduling(table, [&](auto, auto) {
      ++sum;
    });
    benchmark::DoNotOptimize(batching_result);
    benchmark::DoNotOptimize(sum);
  }

  if (use_scheduler) {
    Hyrise::get().scheduler()->finish();
  }
}

BENCHMARK(BM_ChunkGroupingForScheduling)
    ->Arg(0)   // Single-threaded
    ->Arg(1);  // Multi-threaded

}  // namespace hyrise
