#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"

namespace opossum {

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_Sort)(benchmark::State& state) {
  _clear_cache();

  const auto sort_definitions = std::vector<SortColumnDefinition>{{ColumnID{0}, OrderByMode::Ascending}};

  auto warm_up = std::make_shared<Sort>(_table_wrapper_a, sort_definitions);
  warm_up->execute();
  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(_table_wrapper_a, sort_definitions);
    sort->execute();
  }
}

}  // namespace opossum
