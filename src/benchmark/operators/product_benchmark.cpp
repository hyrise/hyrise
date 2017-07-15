#include <memory>
#include <string>
#include <utility>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/product.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Product)(benchmark::State& state) {
  clear_cache();
  auto warm_up = std::make_shared<Product>(_table_wrapper_a, _table_wrapper_b);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto product = std::make_shared<Product>(_table_wrapper_a, _table_wrapper_b);
    product->execute();
  }
}
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Product)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
