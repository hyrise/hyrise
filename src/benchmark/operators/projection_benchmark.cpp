#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "../benchmark_basic_fixture.hpp"
#include "operators/pqp_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "table_generator.hpp"

namespace opossum {

void benchmark_projection_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
                              Projection::ColumnExpressions expressions) {
  auto warm_up = std::make_shared<Projection>(in, expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(in, expressions);
    projection->execute();
  }
}

BENCHMARK_F(BenchmarkBasicFixture, BM_Projection_Simple)(benchmark::State& state) {
  _clear_cache();

  Projection::ColumnExpressions expressions = {PQPExpression::create_column(ColumnID{0} /* "a" */)};
  benchmark_projection_impl(state, _table_wrapper_a, expressions);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_Projection_VariableTerm)(benchmark::State& state) {
  _clear_cache();

  // "a" + "b"
  Projection::ColumnExpressions expressions = {PQPExpression::create_binary_operator(
      ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}), PQPExpression::create_column(ColumnID{1}))};
  benchmark_projection_impl(state, _table_wrapper_a, expressions);
}

BENCHMARK_F(BenchmarkBasicFixture, BM_Projection_ConstantTerm)(benchmark::State& state) {
  _clear_cache();

  // "a" + 5
  Projection::ColumnExpressions expressions = {PQPExpression::create_binary_operator(
      ExpressionType::Addition, PQPExpression::create_column(ColumnID{0}), PQPExpression::create_literal(5))};
  benchmark_projection_impl(state, _table_wrapper_a, expressions);
}

}  // namespace opossum
