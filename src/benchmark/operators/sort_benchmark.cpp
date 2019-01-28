

#include "../micro_benchmark_basic_fixture.hpp" // NEEDEDINCLUDE
#include "operators/sort.hpp" // NEEDEDINCLUDE
#include "operators/table_wrapper.hpp" // NEEDEDINCLUDE

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Sort)(benchmark::State& state) {
  _clear_cache();

  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
  warm_up->execute();
  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
    sort->execute();
  }
}

}  // namespace opossum
