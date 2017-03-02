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
BENCHMARK_F(BenchmarkFixture, BM_Template)(benchmark::State& state) {
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
*/
}  // namespace opossum
