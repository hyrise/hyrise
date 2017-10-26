#include "benchmark/benchmark.h"
#include "benchmark_basic_fixture.hpp"
// other includes as needed

namespace opossum {
/**
 * For a full documentation of googlebenchmark see https://github.com/google/benchmark
 * This template should give you a short overview of the basic functionalities and important steps to follow.
 * If your benchmark only needs one or two test tables you are fine with the example below.
 * In case you need more customizations you have to write your own fixture.
 * Important is that if you use the state.range() function you should declare a variable with a meaningful name
 * and use this variable in your benchmarks. Thus the code stays readable.

BENCHMARK_F(BenchmarkBasicFixture, BM_Template)(benchmark::State& state) {
  // Google benchmark automatically determines a number of executions. The code that should be measured multiple times
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
