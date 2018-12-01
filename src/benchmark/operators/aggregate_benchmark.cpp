#include <memory>
#include <vector>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

void bm_aggregate_impl(benchmark::State& state, std::shared_ptr<TableWrapper> table, std::vector<ColumnID>& groupby) {
  std::vector<AggregateColumnDefinition> aggregates = {{ColumnID{1} /* "b" */, AggregateFunction::Min}};
  auto warm_up = std::make_shared<Aggregate>(table, aggregates, groupby);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto aggregate = std::make_shared<Aggregate>(table, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_1_Column)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}};
  bm_aggregate_impl(state, _table_wrapper_a, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_2_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}};
  bm_aggregate_impl(state, _table_wrapper_a, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_4_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}};
  bm_aggregate_impl(state, _table_wrapper_a, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_8_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3},
                                   ColumnID{4}, ColumnID{5}, ColumnID{6}, ColumnID{7}};
  bm_aggregate_impl(state, _table_wrapper_a, groupby);
}
}  // namespace opossum
