#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void benchmark_projection_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                               const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto warm_up = std::make_shared<Projection>(in, expressions);
  warm_up->execute();
  for (auto _ : state) {
    auto projection = std::make_shared<Projection>(in, expressions);
    projection->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_Simple)(benchmark::State& state) {
  _clear_cache();

  const auto a = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");

  benchmark_projection_impl(state, _table_wrapper_a, {a});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_VariableTerm)(benchmark::State& state) {
  _clear_cache();

  // "column_1" + "column_2"
  const auto a = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");
  const auto b = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_2");

  benchmark_projection_impl(state, _table_wrapper_a, {add_(a, b)});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_ConstantTerm)(benchmark::State& state) {
  _clear_cache();

  const auto a = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");

  // "a" + 5
  benchmark_projection_impl(state, _table_wrapper_a, {add_(a, 5)});
}

}  // namespace opossum
