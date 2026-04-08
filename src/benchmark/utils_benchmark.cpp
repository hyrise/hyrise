#include <memory>

#include "benchmark/benchmark.h"

#include "hyrise.hpp"
#include "operators/table_wrapper.hpp"
#include "synthetic_table_generator.hpp"
#include "types.hpp"
#include "utils/scheduling_utils.hpp"

namespace hyrise {

static void BM_ChunkGroupingForScheduling(benchmark::State& state) {
  constexpr auto CHUNK_COUNT = size_t{100'000};
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  const auto table = table_generator->generate_table(1, CHUNK_COUNT * 2, ChunkOffset{2});

  auto sum = size_t{0};
  for (auto _ : state) {
    auto jobs = group_chunks_for_scheduling(table, [&](auto, auto) {
      ++sum;
    });
    benchmark::DoNotOptimize(jobs);
    benchmark::DoNotOptimize(sum);
  }
}

BENCHMARK(BM_ChunkGroupingForScheduling);

}  // namespace hyrise
