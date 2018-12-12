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

std::shared_ptr<const Table> pre_aggregate2() {
  const float scale_factor = 1.f;
  std::cout << "Starting to generate TCPH tables with scale factor " << scale_factor << "... ";
  opossum::TpchDbGenerator(scale_factor).generate_and_store();
  std::cout << " done" << std::endl;
  std::cout << "Starting to compute join query part for TPCH-18 (modified)..." << std::endl;
  auto sql = "SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, l_quantity FROM customer, orders, lineitem WHERE o_orderkey in (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey having SUM(l_quantity) > 200) AND c_custkey = o_custkey AND o_orderkey = l_orderkey";
  auto builder = SQLPipelineBuilder{sql}.dont_cleanup_temporaries();
  auto sql_pipeline = std::make_unique<SQLPipeline>(builder.create_pipeline());
  auto result = sql_pipeline->get_result_table();
  std::cout << "Finished to compute join query part for TPCH-18 (modified)" << std::endl;
  std::cout << "created table size is " << result->row_count() << std::endl;
  return result;
}

const auto pre_aggregate() {
    static std::shared_ptr<const Table> ptr;
    if (ptr == nullptr) ptr = pre_aggregate2();
    return ptr;
}

void bm_aggregate_impl(benchmark::State& state, std::vector<ColumnID>& groupby) {
  std::vector<AggregateColumnDefinition> aggregates = {{ColumnID{5} /* "l_quantity" */, AggregateFunction::Sum}};

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
  std::vector<ColumnID> groupby = {ColumnID{1}};
  bm_aggregate_impl(state, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_2_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{1}, ColumnID{2}};
  bm_aggregate_impl(state, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_2_Columns_Independent)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{3}};
  bm_aggregate_impl(state, groupby);
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Aggregate_4_Columns)(benchmark::State& state) {
  _clear_cache();
  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}, ColumnID{2}, ColumnID{3}};
  bm_aggregate_impl(state, groupby);
}
}  // namespace opossum
