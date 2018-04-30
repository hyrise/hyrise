#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void BM_TableScan_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                       ColumnID left_column_id, const PredicateCondition predicate_condition,
                       const AllParameterVariant right_parameter) {
  auto warm_up = std::make_shared<TableScan>(in, left_column_id, predicate_condition, right_parameter);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(in, left_column_id, predicate_condition, right_parameter);
    table_scan->execute();
  }
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  clear_cache();
  BM_TableScan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  clear_cache();
  BM_TableScan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanConstant_OnDict)(benchmark::State& state) {
  clear_cache();
  BM_TableScan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanVariable_OnDict)(benchmark::State& state) {
  clear_cache();
  BM_TableScan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

void BM_TableScanLike(benchmark::State& state, const std::string& column_name, const std::string& pattern) {
  const auto lineitem_table = load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl");

  const auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_wrapper->execute();

  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, lineitem_table->column_id_by_name(column_name),
                                                  PredicateCondition::Like, pattern);
    table_scan->execute();
  }
}

BENCHMARK_CAPTURE(BM_TableScanLike, FindPattern, "l_comment", "%final%");
BENCHMARK_CAPTURE(BM_TableScanLike, MultipleFindPatterns, "l_comment", "%final%requests%");
BENCHMARK_CAPTURE(BM_TableScanLike, StartsWithPattern, "l_shipinstruct", "quickly%");
BENCHMARK_CAPTURE(BM_TableScanLike, EndsWithPattern, "l_comment", "%foxes");
BENCHMARK_CAPTURE(BM_TableScanLike, ComplexPattern, "l_comment", "%quick_y__above%even%");

}  // namespace opossum
