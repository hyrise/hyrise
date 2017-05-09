#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/operators/difference.hpp"
#include "../../lib/operators/get_table.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../base_fixture.cpp"
#include "../table_generator.hpp"

namespace opossum {

BENCHMARK_F(BenchmarkFixture, BM_Difference)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
    difference->execute();
  }
}

}  // namespace opossum
