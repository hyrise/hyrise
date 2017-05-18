#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"

namespace opossum {

class OperatorsProjectionBenchmark : public BenchmarkBasicFixture {
 public:
  virtual void SetUp(const ::benchmark::State& state) {
    _table_ref = std::make_shared<TableScan>(_table_wrapper_a, "a", ">=", 0);  // all
    _table_ref->execute();

    _tables.emplace_back(_table_wrapper_a);  // 0
    _tables.emplace_back(_table_wrapper_b);  // 1
    _tables.emplace_back(_table_ref);        // 2
  }

  virtual void TearDown(const ::benchmark::State& state) { StorageManager::get().reset(); }

 protected:
  std::shared_ptr<TableScan> _table_ref;
  std::vector<std::shared_ptr<AbstractOperator>> _tables;
};

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)(benchmark::State& state) {
  clear_cache();
  Projection::ProjectionDefinitions definitions = {Projection::ProjectionDefinition{"$a", "int", "sum"}};
  auto warm_up = std::make_shared<Projection>(_tables[state.range(0)], definitions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_tables[state.range(0)], definitions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)(benchmark::State& state) {
  clear_cache();
  Projection::ProjectionDefinitions definitions = {Projection::ProjectionDefinition{"$a+$b", "int", "sum"}};
  auto warm_up = std::make_shared<Projection>(_tables[state.range(0)], definitions);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto projection = std::make_shared<Projection>(_tables[state.range(0)], definitions);
    projection->execute();
  }
}

BENCHMARK_DEFINE_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)(benchmark::State& state) {
  clear_cache();
  Projection::ProjectionDefinitions definitions = {Projection::ProjectionDefinition{"$a+5", "int", "sum"}};
  auto warm_up = std::make_shared<Projection>(_tables[state.range(0)], definitions);
  warm_up->execute();
  while (state.KeepRunning()) {
    Projection::ProjectionDefinitions definitions = {Projection::ProjectionDefinition{"$a+5", "int", "sum"}};
    auto projection = std::make_shared<Projection>(_tables[state.range(0)], definitions);
    projection->execute();
  }
}

static void ColumnArguments(benchmark::internal::Benchmark* b) {
  for (int i = 0; i <= 2; i++) b->Args({i});  // i = column type
}

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionSimple)->Apply(ColumnArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionVariableTerm)->Apply(ColumnArguments);

BENCHMARK_REGISTER_F(OperatorsProjectionBenchmark, BM_ProjectionConstantTerm)->Apply(ColumnArguments);

}  // namespace opossum
