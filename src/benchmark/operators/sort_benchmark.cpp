#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/sort.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkBasicFixture, BM_Sort)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, "a");
  warm_up->execute();
  while (state.KeepRunning()) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, "a");
    sort->execute();
  }
}

}  // namespace opossum
