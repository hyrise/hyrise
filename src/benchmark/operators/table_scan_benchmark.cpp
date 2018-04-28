#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "table_generator.hpp"

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

}  // namespace opossum
