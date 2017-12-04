#include "benchmark/benchmark.h"

#include "tpch/tpch_db_generator.hpp"

namespace opossum {

/**
 * This benchmark can only be use as a starting point for investigating TpchDbGenerator performance, since secondary
 * invocations of 'TpchDbGenerator(scale_factor, 1000).generate();' will profit from cached data in tpch-dbgen
 * @param state
 */
static void BM_TpchDbGenerator(benchmark::State& state) {
  /**
   * TPCH scale factor is roughly in GB
   */
  const auto mega_bytes = state.range(0);
  const auto scale_factor = static_cast<float>(mega_bytes) / 1000.0f;

  while (state.KeepRunning()) {
    TpchDbGenerator(scale_factor, 1000).generate();
  }
}
BENCHMARK(BM_TpchDbGenerator)->Range(1, 1024);
}  // namespace opossum
