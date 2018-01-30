#include <memory>

#include "../benchmark_basic_fixture.hpp"
#include "../table_generator.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  clear_cache();

  auto warm_up =
      std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanConstantOnDict)(benchmark::State& state) {
  clear_cache();
  auto warm_up =
      std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */, PredicateCondition::GreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  clear_cache();
  auto warm_up =
      std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, ColumnID{1});
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanVariableOnDict)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                             PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_dict_wrapper, ColumnID{0} /* "a" */,
                                                  PredicateCondition::GreaterThanEquals, ColumnID{1} /* "b" */);
    table_scan->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanConstant)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanConstantOnDict)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanVariable)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanVariableOnDict)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
