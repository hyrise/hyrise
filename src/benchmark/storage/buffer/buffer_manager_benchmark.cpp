#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "micro_benchmark_basic_fixture.hpp"
#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

// void benchmark_projection_impl(benchmark::State& state, const std::shared_ptr<const AbstractOperator> in,
//                                const std::vector<std::shared_ptr<AbstractExpression>>& expressions) {
//   auto warm_up = std::make_shared<Projection>(in, expressions);
//   warm_up->execute();
//   for (auto _ : state) {
//     auto projection = std::make_shared<Projection>(in, expressions);
//     projection->execute();
//   }
// }

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_Simple)(benchmark::State& state) {
  // _clear_cache();

  // const auto expression = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");

  //   auto warm_up = std::make_shared<Projection>(_table_wrapper_a, {expression});
  // warm_up->execute();
  // for (auto _ : state) {
  //   auto projection = std::make_shared<Projection>(_table_wrapper_a, {expression});
  //   projection->execute();
  // }
}
}  // namespace hyrise
