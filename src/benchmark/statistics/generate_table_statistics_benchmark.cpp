#include "benchmark/benchmark.h"

#include "micro_benchmark_basic_fixture.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace opossum {

BENCHMARK_DEFINE_F(MicroBenchmarkBasicFixture, BM_GenerateTableStatistics_TPCH)(benchmark::State& state) {
  _clear_cache();

  TpchTableGenerator{state.range(0) / 1000.0f}.generate_and_store();

  for (auto _ : state) {
    for (const auto& pair : StorageManager::get().tables()) {
      generate_table_statistics(*pair.second);
    }
  }
}

// Args are scale_factor * 1000 since Args only takes ints
BENCHMARK_REGISTER_F(MicroBenchmarkBasicFixture, BM_GenerateTableStatistics_TPCH)->Range(10, 750);

}  // namespace opossum
