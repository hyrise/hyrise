#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "operators/difference.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Difference)(benchmark::State& state) {
  _clear_cache();
  auto warm_up = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  for (auto _ : state) {
    auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
    difference->execute();
  }
}

}  // namespace opossum
