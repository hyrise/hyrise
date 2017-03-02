#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/table_scan.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../base_fixture.cpp"
#include "../table_generator.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkFixture, BM_TableScan)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<TableScan>(_gt_a, "a", ">=", 7);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto table_scan = std::make_shared<TableScan>(_gt_a, "a", ">=", 7);
    table_scan->execute();
  }
}

}  // namespace opossum
