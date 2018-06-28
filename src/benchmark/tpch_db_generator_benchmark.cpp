#include "benchmark/benchmark.h"

#include "tpch/tpch_db_generator.hpp"

namespace opossum {

/**
 * This benchmark can only be use as a starting point for investigating TpchDbGenerator performance, since secondary
 * invocations of 'TpchDbGenerator(scale_factor, 1000).generate();' will profit from cached data in tpch-dbgen
 * @param state
 */
static void BM_TpchDbGenerator(benchmark::State& state) {  // NOLINT
  while (state.KeepRunning()) {
    TpchDbGenerator(0.5f, 1000).generate();
  }
}
BENCHMARK(BM_TpchDbGenerator);

}  // namespace opossum
