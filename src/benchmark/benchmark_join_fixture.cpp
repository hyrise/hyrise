#include "benchmark_join_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"

namespace opossum {

void BenchmarkJoinFixture::SetUp(::benchmark::State& state) {
  // Generating a test table with generate_table function from table_generator.cpp

  auto table_generator = std::make_shared<TableGenerator>();

  auto table_1 = table_generator->generate_table(static_cast<ChunkID>(state.range(0)), true);
  auto table_2 = table_generator->generate_table(static_cast<ChunkID>(state.range(1)), true);

  for(auto table : {table_1, table_2}){
    for (ChunkID chunk_id{0}; chunk_id < table->chunk_count(); ++chunk_id) {
      auto chunk = table->get_chunk(chunk_id);

      std::vector<ColumnID> columns{1};
      for (ColumnID column_id{0}; column_id < chunk->column_count(); ++column_id) {
        columns[0] = column_id;
        chunk->create_index<AdaptiveRadixTreeIndex>(columns);
      }
    }
  }

  _tw_small_uni1 = std::make_shared<TableWrapper>(table_1);
  _tw_small_uni2 = std::make_shared<TableWrapper>(table_2);
  _tw_small_uni1->execute();
  _tw_small_uni2->execute();
}

void BenchmarkJoinFixture::TearDown(::benchmark::State&) { opossum::StorageManager::get().reset(); }

void BenchmarkJoinFixture::ChunkSizeIn(benchmark::internal::Benchmark* b) {
  for (ChunkID i : {ChunkID(50), ChunkID(500), ChunkID(5000)}) {
    for (ChunkID j : {ChunkID(50), ChunkID(500), ChunkID(5000)}) {
      b->Args({static_cast<int>(i), static_cast<int>(j)});  // i = chunk size
    }
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
