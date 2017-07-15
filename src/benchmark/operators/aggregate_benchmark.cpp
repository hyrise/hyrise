#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "../base_fixture.cpp"
#include "../table_generator.hpp"
#include "operators/aggregate.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(BenchmarkBasicFixture, BM_Aggregate)(benchmark::State& state) {
  clear_cache();

  std::vector<AggregateDefinition> aggregates = {{"b", Min}};
  std::vector<std::string> groupby = {std::string("a")};

  auto warm_up = std::make_shared<Aggregate>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  while (state.KeepRunning()) {
    auto aggregate = std::make_shared<Aggregate>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}
BENCHMARK_REGISTER_F(BenchmarkBasicFixture, BM_Aggregate)->Apply(BenchmarkBasicFixture::ChunkSizeIn);

}  // namespace opossum
