#include <memory>

#include "benchmark/benchmark.h"

#include "../base_fixture.hpp"
#include "../table_generator.hpp"

#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  clear_cache();

  auto warm_up = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, ScanType::OpGreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */, ScanType::OpGreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0}, ScanType::OpGreaterThanEquals, ColumnID{1});
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, ColumnID{0} /* "a" */,
                                                  ScanType::OpGreaterThanEquals, ColumnID{1} /* "b" */);
    table_scan->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanConstant)->Apply(BenchmarkBasicFixture::ChunkSizeIn);
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_TableScanVariable)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
