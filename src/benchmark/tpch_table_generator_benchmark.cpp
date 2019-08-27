#include "benchmark/benchmark.h"

#include "hyrise.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace opossum {

/**
 * This benchmark can only be use as a starting point for investigating TPCHTableGenerator performance, since secondary
 * invocations of 'TPCHTableGenerator(scale_factor, 1000).generate();' will profit from cached data in tpch-dbgen
 * @param state
 */
static void BM_TPCHTableGenerator(benchmark::State& state) {  // NOLINT
  for (auto _ : state) {
    TPCHTableGenerator(0.5f, 1000).generate_and_store();
    Hyrise::reset();
  }
}
BENCHMARK(BM_TPCHTableGenerator);

}  // namespace opossum
