#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/sort.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Sort_ChunkSize)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, "a", state.range(0));
  warm_up->execute();
  while (state.KeepRunning()) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, "a", state.range(0));
    sort->execute();
  }
}

static void ChunkSize(benchmark::internal::Benchmark* b) {
  for (int i : {0, 100}) b->Args({i});  // i = chunk size
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Sort_ChunkSize)->Apply(ChunkSize);

}  // namespace opossum
