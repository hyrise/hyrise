#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

namespace opossum {

void benchmark_tablescan_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                              ColumnID left_column_id, const PredicateCondition predicate_condition,
                              const AllParameterVariant right_parameter) {
  auto warm_up =
      std::make_shared<TableScan>(in, OperatorScanPredicate{left_column_id, predicate_condition, right_parameter});
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<TableScan>(in, OperatorScanPredicate{left_column_id, predicate_condition, right_parameter});
    table_scan->execute();
  }
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanConstant_OnDict)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, 7);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanVariable_OnDict)(benchmark::State& state) {
  _clear_cache();
  benchmark_tablescan_impl(state, _table_dict_wrapper, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScan_Like)(benchmark::State& state) {
  const auto lineitem_table = load_table("src/test/tables/tpch/sf-0.001/lineitem.tbl");

  const auto lineitem_wrapper = std::make_shared<TableWrapper>(lineitem_table);
  lineitem_wrapper->execute();

  const auto column_names_and_patterns = std::vector<std::pair<std::string, std::string>>({
      {"l_comment", "%final%"},
      {"l_comment", "%final%requests%"},
      {"l_shipinstruct", "quickly%"},
      {"l_comment", "%foxes"},
      {"l_comment", "%quick_y__above%even%"},
  });

  while (state.KeepRunning()) {
    for (const auto& column_name_and_pattern : column_names_and_patterns) {
      auto table_scan = std::make_shared<TableScan>(
          lineitem_wrapper, OperatorScanPredicate{lineitem_table->column_id_by_name(column_name_and_pattern.first),
                                                  PredicateCondition::Like, column_name_and_pattern.second});
      table_scan->execute();
    }
  }
}

}  // namespace opossum
