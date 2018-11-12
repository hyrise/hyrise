#include <memory>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"
#include "table_generator.hpp"

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_UnionAll)(benchmark::State& state) {
  _clear_cache();
  auto warm_up = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto union_all = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
    union_all->execute();
  }
}

}  // namespace opossum
