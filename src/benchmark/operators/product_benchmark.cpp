#include <memory>

#include "benchmark/benchmark.h"

#include "../base_fixture.hpp"
#include "../table_generator.hpp"
#include "operators/product.hpp"
#include "operators/table_wrapper.hpp"

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
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Product)
    ->Args({0})
    ->Args({10000});  // for this benchmark only tables with a chunk_size of 0 and 10 000 are used. A product operation
                      // on two tables with chunk_size of 100 000 takes about one hour

}  // namespace opossum
