#include "benchmark/micro_benchmark_basic_fixture.hpp" // NEEDEDINCLUDE
#include "operators/difference.hpp" // NEEDEDINCLUDE
#include "operators/table_wrapper.hpp" // NEEDEDINCLUDE

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
