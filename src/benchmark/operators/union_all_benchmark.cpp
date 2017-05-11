#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../../lib/operators/get_table.hpp"
#include "../../lib/operators/union_all.hpp"
#include "../../lib/storage/storage_manager.hpp"
#include "../../lib/storage/table.hpp"
#include "../../lib/types.hpp"
#include "../base_fixture.cpp"
#include "../table_generator.hpp"

namespace opossum {

// BENCHMARK_F(BenchmarkFixture, BM_UnionAll)(benchmark::State& state) {
//  clear_cache();
//  auto warm_up = std::make_shared<UnionAll>(_gt_a, _gt_b);
//  warm_up->execute();
//  while (state.KeepRunning()) {
//    auto union_all = std::make_shared<UnionAll>(_gt_a, _gt_b);
//    union_all->execute();
//  }
//}

}  // namespace opossum
