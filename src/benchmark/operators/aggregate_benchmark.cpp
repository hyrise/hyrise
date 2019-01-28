#include "benchmark/micro_benchmark_basic_fixture.hpp" // NEEDEDINCLUDE
#include "operators/aggregate.hpp" // NEEDEDINCLUDE
#include "operators/table_wrapper.hpp" // NEEDEDINCLUDE

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate)(benchmark::State& state) {
  _clear_cache();

  std::vector<AggregateColumnDefinition> aggregates = {{ColumnID{1} /* "b" */, AggregateFunction::Min}};

  std::vector<ColumnID> groupby = {ColumnID{0} /* "a" */};

  auto warm_up = std::make_shared<Aggregate>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<Aggregate>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}

}  // namespace opossum
