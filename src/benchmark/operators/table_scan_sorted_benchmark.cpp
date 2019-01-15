#include <memory>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "type_cast.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

const int string_size = 512;

void _clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

std::shared_ptr<TableColumnDefinitions> create_column_definitions(const DataType data_type) {
  auto table_column_definitions = std::make_shared<TableColumnDefinitions>();

  table_column_definitions->emplace_back("ascending", data_type, true);
  table_column_definitions->emplace_back("ascending_nulls_last", data_type, true);
  table_column_definitions->emplace_back("descending", data_type, true);
  table_column_definitions->emplace_back("descending_nulls_last", data_type, true);

  return table_column_definitions;
}

std::shared_ptr<TableWrapper> create_int_table(const int table_size, const bool set_sorted_flag) {
  const auto table_column_definitions = create_column_definitions(DataType::Int);

  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;

  table = std::make_shared<Table>(*table_column_definitions, TableType::Data);

  for (int i = 0; i < table_size; i++) {
    table->append({i, i, table_size - i - 1, table_size - i - 1});
  }

  if (set_sorted_flag) {
    for (auto& chunk : table->chunks()) {
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

std::shared_ptr<TableWrapper> create_string_table(const int table_size, const int string_length,
                                                  const bool set_sorted_flag) {
  const auto table_column_definitions = create_column_definitions(DataType::String);
  std::shared_ptr<Table> table;
  std::shared_ptr<TableWrapper> table_wrapper;

  table = std::make_shared<Table>(*table_column_definitions, TableType::Data);

  for (int i = 0; i < table_size; i++) {
    auto str1 = std::to_string(i);
    str1 = std::string(string_length - str1.length(), '0').append(str1);
    auto str2 = std::to_string(table_size - i - 1);
    str2 = std::string(string_length - str2.length(), '0').append(str2);
    table->append({str1, str1, str2, str2});
  }

  if (set_sorted_flag) {
    for (auto& chunk : table->chunks()) {
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

void BM_TableScanSorted(benchmark::State& state, const int table_size, const float selectivity,
                        const PredicateCondition predicate_condition,
                        std::function<std::shared_ptr<TableWrapper>(const int)> table_creator) {
  _clear_cache();

  AllTypeVariant search_value;
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
      // TODO(cmfcmf) Improve logic for other predicates.
      Fail("Unsupported predicate condition");
  }
  const auto table_wrapper = table_creator(table_size);
  const auto table_column_definitions = table_wrapper->get_output()->column_definitions();

  const auto column_index = ColumnID(0);

  const auto column_definition = table_column_definitions.at(column_index);

  if (column_definition.data_type == DataType::String) {
    const auto str = std::to_string(type_cast_variant<int>(search_value));
    search_value = std::string(string_size - str.length(), '0').append(str);
  }

  const auto column_expression =
      pqp_column_(column_index, column_definition.data_type, column_definition.nullable, column_definition.name);

  auto predicate =
      std::make_shared<BinaryPredicateExpression>(predicate_condition, column_expression, value_(search_value));

  auto warm_up = std::make_shared<TableScan>(table_wrapper, predicate);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(table_wrapper, predicate);
    table_scan->execute();
  }
}

std::shared_ptr<TableWrapper> sorted_int_table(const int table_size) {
  static auto table_wrapper = create_int_table(table_size, true);
  return table_wrapper;
}

std::shared_ptr<TableWrapper> unsorted_int_table(const int table_size) {
  static auto table_wrapper = create_int_table(table_size, false);
  return table_wrapper;
}

std::shared_ptr<TableWrapper> sorted_string_table(const int table_size, const int string_length) {
  static auto table_wrapper = create_string_table(table_size, string_length, true);
  return table_wrapper;
}

std::shared_ptr<TableWrapper> unsorted_string_table(const int table_size, const int string_length) {
  static auto table_wrapper = create_string_table(table_size, string_length, false);
  return table_wrapper;
}

const auto rows = 1'000'000;

BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted01, rows, 0.001, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted01, rows, 0.001, PredicateCondition::LessThanEquals,
                  unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted1, rows, 0.01, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted1, rows, 0.01, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted10, rows, 0.1, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted10, rows, 0.1, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted30, rows, 0.3, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted30, rows, 0.3, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted50, rows, 0.5, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted50, rows, 0.5, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted70, rows, 0.7, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted70, rows, 0.7, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted90, rows, 0.9, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted90, rows, 0.9, PredicateCondition::LessThanEquals, unsorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntSorted99, rows, 0.99, PredicateCondition::LessThanEquals, sorted_int_table);
BENCHMARK_CAPTURE(BM_TableScanSorted, IntUnSorted99, rows, 0.99, PredicateCondition::LessThanEquals,
                  unsorted_int_table);


BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted01, rows, 0.001, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted01, rows, 0.001, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted1, rows, 0.01, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted1, rows, 0.01, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted10, rows, 0.1, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted10, rows, 0.1, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted30, rows, 0.3, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted30, rows, 0.3, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted50, rows, 0.5, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted50, rows, 0.5, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted70, rows, 0.7, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted70, rows, 0.7, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted90, rows, 0.9, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted90, rows, 0.9, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringSorted99, rows, 0.99, PredicateCondition::LessThanEquals,
                  std::bind(sorted_string_table, std::placeholders::_1, string_size));
BENCHMARK_CAPTURE(BM_TableScanSorted, StringUnSorted99, rows, 0.99, PredicateCondition::LessThanEquals,
                  std::bind(unsorted_string_table, std::placeholders::_1, string_size));

}  // namespace opossum
