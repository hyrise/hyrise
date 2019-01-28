#include "benchmark/micro_benchmark_basic_fixture.hpp" // NEEDEDINCLUDE
#include "operators/table_wrapper.hpp" // NEEDEDINCLUDE
#include "operators/union_all.hpp" // NEEDEDINCLUDE

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_UnionAll)(benchmark::State& state) {
  _clear_cache();
  auto warm_up = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  for (auto _ : state) {
    auto union_all = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
    union_all->execute();
  }
}

}  // namespace opossum
