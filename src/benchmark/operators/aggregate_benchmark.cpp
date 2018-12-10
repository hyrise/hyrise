#include <memory>
#include <vector>
#include <operators/join_hash.hpp>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/aggregate.hpp"
#include "operators/table_wrapper.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "types.hpp"

namespace opossum {

    std::shared_ptr<const Table> pre_aggregate() {
      opossum::TpchDbGenerator(0.1f).generate_and_store();
  auto sql = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity FROM customer, orders, lineitem WHERE o_orderkey in (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey having SUM(l_quantity) > 300) AND c_custkey = o_custkey AND o_orderkey = l_orderkey";
  auto builder = SQLPipelineBuilder{sql}.dont_cleanup_temporaries();
  auto sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
  return sql_pipeline->get_result_table();
}

void bm_aggregate_impl(benchmark::State& state, std::vector<ColumnID>& groupby) {
  std::vector<AggregateColumnDefinition> aggregates = {{ColumnID{5} /* "l_quantity" */, AggregateFunction::Sum}};
  // auto tables = opossum::TpchDbGenerator(0.1f).generate();
  // auto line_item_table = tables.at(TpchTable::LineItem);
  // auto order_table = tables.at(TpchTable::Orders);


  const auto pre_result = std::make_shared<TableWrapper>(pre_aggregate());
  pre_result->execute();

  auto warm_up = std::make_shared<Aggregate>(pre_result, aggregates, groupby);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto aggregate = std::make_shared<Aggregate>(pre_result, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_1_Column)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}};
  bm_aggregate_impl(state, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_2_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}};
  bm_aggregate_impl(state, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_4_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}};
  bm_aggregate_impl(state, groupby);
}
}  // namespace opossum
