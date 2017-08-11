#include <memory>

#include "benchmark/benchmark.h"

#include "../base_fixture.hpp"
#include "../table_generator.hpp"

#include "operators/difference.hpp"
#include "operators/table_wrapper.hpp"

#include "types.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Difference)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto difference = std::make_shared<Difference>(_table_wrapper_a, _table_wrapper_b);
    difference->execute();
  }
}
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Difference)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
