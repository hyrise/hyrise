#include <memory>

#include "benchmark/benchmark.h"

#include "micro_benchmark_basic_fixture.hpp"
#include "operators/difference.hpp"
// This header is needed for the conversion from TableWrapper to AbstractOperator
#include "operators/table_wrapper.hpp"  // IWYU pragma: keep

namespace hyrise {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Difference)(benchmark::State& state) {
  _clear_cache();
  auto warm_up = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
    difference->execute();
  }
}

}  // namespace hyrise
