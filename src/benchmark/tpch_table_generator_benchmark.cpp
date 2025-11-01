#include "benchmark/benchmark.h"

#include "hyrise.hpp"
#include "tpch/tpch_constants.hpp"
#include "tpch/tpch_table_generator.hpp"

namespace hyrise {

namespace {
/**
 * This benchmark can only be used as a starting point for investigating TPCHTableGenerator performance, since secondary
 * invocations of 'TPCHTableGenerator(scale_factor, 1000).generate();' will profit from cached data in tpch-dbgen
 * @param state.
 */
void bm_tpch_table_generator(benchmark::State& state) {
  // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores)
  for (auto _ : state) {
    TPCHTableGenerator(0.5, ClusteringConfiguration::None, ChunkOffset{1000}).generate_and_store();
    Hyrise::reset();
  }
}

}  // namespace

BENCHMARK(bm_tpch_table_generator);

}  // namespace hyrise
