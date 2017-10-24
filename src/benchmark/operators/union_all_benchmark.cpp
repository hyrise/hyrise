#include <memory>

#include "../base_fixture.hpp"
#include "../table_generator.hpp"
#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "operators/union_all.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_UnionAll)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto union_all = std::make_shared<UnionAll>(_table_wrapper_a, _table_wrapper_b);
    union_all->execute();
  }
}

BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_UnionAll)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
