#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/projection.hpp"
#include "operators/table_wrapper.hpp"

namespace {
using namespace hyrise;  // NOLINT(build/namespaces)

void benchmark_projection_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator>& input,
                               const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
  auto warm_up = std::make_shared<Projection>(input, expressions);
  warm_up->execute();
  for (auto _ : state) {
    auto projection = std::make_shared<Projection>(input, expressions);
    projection->execute();
  }
}

}  // namespace

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_Simple)(benchmark::State& state) {
  _clear_cache();

  const auto column_1 = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");

  benchmark_projection_impl(state, _table_wrapper_a, {column_1});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_VariableTerm)(benchmark::State& state) {
  _clear_cache();

  // "column_1" + "column_2"
  const auto column_1 = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");
  const auto column_2 = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_2");

  benchmark_projection_impl(state, _table_wrapper_a, {add_(column_1, column_2)});
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_ConstantTerm)(benchmark::State& state) {
  _clear_cache();

  const auto column_1 = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");

  // "a" + 5
  benchmark_projection_impl(state, _table_wrapper_a, {add_(column_1, 5)});
}

}  // namespace hyrise
