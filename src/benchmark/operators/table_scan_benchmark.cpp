#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/table_scan.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanConstant)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<TableScan>(_table_wrapper_a, ColumnName("a"), ScanType::OpGreaterThanEquals, 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_table_wrapper_a, ColumnName("a"), ScanType::OpGreaterThanEquals, 7);
    table_scan->execute();
  }
}

BENCHMARK_F(BenchmarkBasicFixture, BM_TableScanVariable)(benchmark::State& state) {
  clear_cache();

  auto warm_up =
      std::make_shared<TableScan>(_table_wrapper_a, ColumnName("a"), ScanType::OpGreaterThanEquals, ColumnName("b"));
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan =
        std::make_shared<TableScan>(_table_wrapper_a, ColumnName("a"), ScanType::OpGreaterThanEquals, ColumnName("b"));
    table_scan->execute();
  }
}

}  // namespace opossum
