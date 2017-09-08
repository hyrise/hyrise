#include <memory>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"

#include "../base_fixture.hpp"
#include "../table_generator.hpp"

#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

class OperatorsProjectionBenchmark : public BenchmarkBasicFixture {
 public:
  void SetUp(::benchmark::State& state) override {
    BenchmarkBasicFixture::SetUp(state);
    _column_type = state.range(1);

    _table_ref =
        std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, ScanType::OpGreaterThanEquals, 0);  // all
    _table_ref->execute();

    _tables.emplace_back(_table_wrapper_a);  // 0
    _tables.emplace_back(_table_wrapper_b);  // 1
    _tables.emplace_back(_table_ref);        // 2
  }

 protected:
  std::shared_ptr<TableScan> _table_ref;
  std::vector<std::shared_ptr<AbstractOperator>> _tables;
  int _column_type;
};

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)(benchmark::State& state) {
  clear_cache();

  Projection::ColumnExpressions expressions = {Expression::create_column(ColumnID{0} /* "a" */)};
  auto warm_up = std::make_shared<Projection>(_tables[_column_type], expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_tables[_column_type], expressions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)(benchmark::State& state) {
  clear_cache();
  // "a" + "b"
  Projection::ColumnExpressions expressions = {Expression::create_binary_operator(
      ExpressionType::Addition, Expression::create_column(ColumnID{0}), Expression::create_column(ColumnID{1}))};
  auto warm_up = std::make_shared<Projection>(_tables[_column_type], expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_tables[_column_type], expressions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)(benchmark::State& state) {
  clear_cache();

  // "a" + 5
  Projection::ColumnExpressions expressions = {Expression::create_binary_operator(
      ExpressionType::Addition, Expression::create_column(ColumnID{0}), Expression::create_literal(5))};
  auto warm_up = std::make_shared<Projection>(_tables[_column_type], expressions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_tables[_column_type], expressions);
    projection->execute();
  }
}

static void CustomArguments(benchmark::internal::Benchmark* b) {
  for (ChunkID chunk_size : {ChunkID(0), ChunkID(10000), ChunkID(100000)}) {
    for (int column_type = 0; column_type <= 2; column_type++) {
      b->Args({static_cast<int>(chunk_size), column_type});
    }
  }
}

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)->Apply(CustomArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)->Apply(CustomArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)->Apply(CustomArguments);

}  // namespace opossum
