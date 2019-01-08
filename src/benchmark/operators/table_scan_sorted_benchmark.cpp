#include <memory>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::shared_ptr<TableColumnDefinitions> create_column_definitions() {
  auto table_column_definitions = std::make_shared<TableColumnDefinitions>();

  table_column_definitions->emplace_back("ascending", DataType::Int, true);
  table_column_definitions->emplace_back("ascending_nulls_last", DataType::Int, true);
  table_column_definitions->emplace_back("descending", DataType::Int, true);
  table_column_definitions->emplace_back("descending_nulls_last", DataType::Int, true);

  return table_column_definitions;
}

std::shared_ptr<TableWrapper> create_table(const std::shared_ptr<TableColumnDefinitions> table_column_definitions, const int table_size, const bool set_sorted_flag) {
  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;

  table = std::make_shared<Table>(*table_column_definitions, TableType::Data);

  for (int i = 0; i < table_size; i++) {
    table->append({i, i, table_size - i - 1, table_size - i - 1});
  }

  if (set_sorted_flag) {
    for (auto &chunk : table->chunks()) {
      chunk->get_segment(ColumnID(0))->set_sort_order(OrderByMode::Ascending);
      chunk->get_segment(ColumnID(1))->set_sort_order(OrderByMode::AscendingNullsLast);
      chunk->get_segment(ColumnID(2))->set_sort_order(OrderByMode::Descending);
      chunk->get_segment(ColumnID(3))->set_sort_order(OrderByMode::DescendingNullsLast);
    }
  }

  table_wrapper = std::make_shared<TableWrapper>(std::move(table));
  table_wrapper->execute();

  return table_wrapper;
}


void BM_TableScanSorted(benchmark::State& state, const int table_size, const float selectivity, const PredicateCondition predicate_condition, bool mark_as_sorted) {
  // const int table_size = std::get<0>(std::forward<ExtraArgs>(extra_args)...);
  // const float selectivity = std::get<1>(std::forward<ExtraArgs>(extra_args)...);
  // const PredicateCondition predicate_condition = std::get<2>(std::forward<ExtraArgs>(extra_args)...);
  // const bool mark_as_sorted = std::get<3>(std::forward<ExtraArgs>(extra_args)...);

  int search_value = -1;
  switch (predicate_condition) {
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::LessThan:
      search_value = static_cast<int>(table_size * selectivity);
      break;
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      search_value = static_cast<int>(table_size - table_size * selectivity);
      break;
    default:
      // TODO Improve logic
      Fail("Unsupported predicate condition");
  }
  const auto table_column_definitions = create_column_definitions();
  const auto table_wrapper = create_table(table_column_definitions, table_size, mark_as_sorted);

  const auto column_index = ColumnID(0);

  const auto column_definition = table_column_definitions->at(column_index);
  const auto column_expression =
      pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

  auto predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, value_(search_value));

  auto warm_up = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

const auto rows = 10'000'000;

BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted01, rows, 0.001, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted10, rows, 0.1, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted30, rows, 0.3, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted50, rows, 0.5, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted70, rows, 0.7, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted90, rows, 0.9, PredicateCondition::LessThanEquals, true);
BENCHMARK_CAPTURE(BM_TableScanSorted, Sorted99, rows, 0.99, PredicateCondition::LessThanEquals, true);

BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted01, rows, 0.001, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted10, rows, 0.1, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted30, rows, 0.3, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted50, rows, 0.5, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted70, rows, 0.7, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted90, rows, 0.9, PredicateCondition::LessThanEquals, false);
BENCHMARK_CAPTURE(BM_TableScanSorted, UnSorted99, rows, 0.99, PredicateCondition::LessThanEquals, false);

}  // namespace opossum
