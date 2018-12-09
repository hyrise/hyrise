#include "benchmark/benchmark.h"

#include "tpch/tpch_table_generator.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

/**
 * This benchmark can only be use as a starting point for investigating TpchTableGenerator performance, since secondary
 * invocations of 'TpchTableGenerator(scale_factor, 1000).generate();' will profit from cached data in tpch-dbgen
 * @param state
 */
static void BM_TpchDbGenerator(benchmark::State& state) {  // NOLINT
  while (state.KeepRunning()) {
    TpchTableGenerator(0.5f, 1000).generate_and_store();
    StorageManager::reset();
  }
}
BENCHMARK(BM_TpchDbGenerator);

}  // namespace opossum
