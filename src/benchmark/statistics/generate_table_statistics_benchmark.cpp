#include "benchmark/benchmark.h"

#include "micro_benchmark_basic_fixture.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "tpch/tpch_db_generator.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_GenerateTableStatistics_TPCH)(benchmark::State& state) {
  _clear_cache();

  const auto tables = TpchDbGenerator{state.range(0) / 1000.0f}.generate();

  while (state.KeepRunning()) {
    for (const auto& pair : tables) {
      generate_table_statistics(*pair.second);
    }
  }
}

// Args are scale_factor * 1000 since Args only takes ints
BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_GenerateTableStatistics_TPCH)->Range(10, 750);

}  // namespace opossum
