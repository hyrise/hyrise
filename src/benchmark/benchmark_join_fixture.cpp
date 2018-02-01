#include "benchmark_join_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace opossum {

void BenchmarkJoinFixture::SetUp(::benchmark::State& state) {
  // Generating a test table with generate_table function from table_generator.cpp
  _chunk_size = static_cast<ChunkID>(state.range(0));

  auto table_generator = std::make_shared<TableGenerator>();

  auto table_generator2 = std::make_shared<TableGenerator>();

  _tw_small_uni1 = std::make_shared<TableWrapper>(table_generator->generate_table(_chunk_size));
  _tw_small_uni2 = std::make_shared<TableWrapper>(table_generator2->generate_table(_chunk_size));
  _tw_small_uni1->execute();
  _tw_small_uni2->execute();
}

void BenchmarkJoinFixture::TearDown(::benchmark::State&) { opossum::StorageManager::get().reset(); }

void BenchmarkJoinFixture::ChunkSizeIn(benchmark::internal::Benchmark* b) {
  for (ChunkID i : {ChunkID(1), ChunkID(10), ChunkID(100)}) {
    b->Args({static_cast<int>(i)});  // i = chunk size
  }
}

void BenchmarkJoinFixture::clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}  // namespace opossum
