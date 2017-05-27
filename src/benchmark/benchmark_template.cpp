#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../lib/operators/difference.hpp"
#include "../lib/operators/get_table.hpp"
#include "../lib/operators/projection.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/types.hpp"
#include "base_fixture.cpp"
#include "table_generator.hpp"

namespace opossum {
/*
BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Template)(benchmark::State& state) {
  // Google benchmark automaticly determines a number of executions. The code that should be measured multiple times
  // goes in the while loop. Executions before the loop won't be measured.
  clear_cache();
  auto warm_up = std::make_shared<Difference>(_gt_a, _gt_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    state.PauseTiming();
    // If you have something to be configured wich shouldn't be measured, you can pause the measures.
    state.ResumeTiming();
    auto difference = std::make_shared<opossum::Difference>(_gt_a, _gt_b);
    difference->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Template)->Apply(BenchmarkBasicFixture::ChunkSizeIn);


// If you like to add your own parameters you need to write your own parameters function like this. It is important that
// the first parameter is always the chunk size of the generated table due to consitent naming.
static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (int i : {0, 10000, 100000}) {
    for (int j = 0; i <= 2; j++) {
      b->Args({i, j});  // i = column type, j = chunk size in
    }
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Template)->Apply(CustomArguments);
*/
}  // namespace opossum
