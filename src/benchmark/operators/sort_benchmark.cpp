#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/sort.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Sort_ChunkSizeOut)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, "a", state.range(1));
  warm_up->execute();
  while (state.KeepRunning()) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, "a", state.range(1));
    sort->execute();
  }
}

static void ChunkSizeOut(benchmark::internal::Benchmark* b) {
  for (ChunkID chunk_size_in : {0, 10000, 100000}) {
    for (ChunkID chunk_size_out : {0, 10000, 100000}) {
      b->Args({chunk_size_in, chunk_size_out});
    }
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Sort_ChunkSizeOut)->Apply(ChunkSizeOut);

}  // namespace opossum
