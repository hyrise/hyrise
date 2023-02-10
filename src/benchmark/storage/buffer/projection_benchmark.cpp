#include <memory>
#include <vector>

#include "benchmark/benchmark.h"

#include "expression/expression_functional.hpp"
#include "expression/pqp_column_expression.hpp"
#include "micro_benchmark_basic_fixture.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/buffer/utils.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Projection_Simple)(benchmark::State& state) {
  // _clear_cache();

  const auto expression = PQPColumnExpression::from_table(*_table_wrapper_a->get_output(), "column_1");
  const auto expressions = std::vector<std::shared_ptr<AbstractExpression>>{expression};

  auto warm_up = std::make_shared<Projection>(_table_wrapper_a, expressions);
  warm_up->execute();

  for (auto _ : state) {
    auto projection = std::make_shared<Projection>(_table_wrapper_a, expressions);
    projection->execute();
  }

  add_buffer_manager_counters(state, BufferManager::get_global_buffer_manager());
}

}  // namespace hyrise
