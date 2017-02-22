#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/sort.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../base_fixture.cpp"
#include "../table_generator.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkFixture, BM_Sort)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Sort>(_gt_a, "a");
  warm_up->execute();
  while (state.KeepRunning()) {
    auto sort = std::make_shared<Sort>(_gt_a, "a");
    sort->execute();
  }
}

}  // namespace opossum
