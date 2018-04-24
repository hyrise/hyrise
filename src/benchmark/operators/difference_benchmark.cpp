#include <memory>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "operators/difference.hpp"
#include "operators/table_wrapper.hpp"
#include "table_generator.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkBasicFixture, BM_Difference)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
    difference->execute();
  }
}

}  // namespace opossum
