#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void benchmark_tablescan_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                              ColumnID left_column_id, const PredicateCondition predicate_condition,
                              const AllParameterVariant right_parameter) {
  const auto left_operand = pqp_column_(left_column_id, in->get_output()->column_data_type(left_column_id),
                                        in->get_output()->column_is_nullable(left_column_id), "");
  auto right_operand = std::shared_ptr<AbstractExpression>{};
  if (right_parameter.type() == typeid(ColumnID)) {
    const auto right_column_id = boost::get<ColumnID>(right_parameter);
    right_operand = pqp_column_(right_column_id, in->get_output()->column_data_type(right_column_id),
                                in->get_output()->column_is_nullable(right_column_id), "");

  } else {
    right_operand = value_(boost::get<AllTypeVariant>(right_parameter));
  }

  const auto predicate = std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand);

  auto warm_up = std::make_shared<TableScan>(in, predicate);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(in, predicate);
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
      const auto column_id = lineitem_table->column_id_by_name(column_name_and_pattern.first);
      const auto column = pqp_column_(column_id, DataType::String, false, "");
      const auto predicate = like_(column, value_(column_name_and_pattern.second));

      auto table_scan = std::make_shared<TableScan>(lineitem_wrapper, predicate);
      table_scan->execute();
    }
  }
}

}  // namespace opossum
