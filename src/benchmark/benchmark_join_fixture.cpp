#include "benchmark_join_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace opossum {

void BenchmarkJoinFixture::SetUp(::benchmark::State& state) {
  // Generating a test table with generate_table function from table_generator.cpp

  auto table_generator = std::make_shared<TableGenerator>();

  DataDistributionType right_distribution = static_cast<DataDistributionType>(state.range(2));

  auto left_config = ColumnDataDistribution::make_uniform_config(0.0, 10000);

  ColumnDataDistribution right_config;

  switch (right_distribution) {
    case DataDistributionType::NormalSkewed:
      right_config = ColumnDataDistribution::make_skewed_normal_config();
      break;
    case DataDistributionType::Pareto:
      right_config = ColumnDataDistribution::make_pareto_config();
      break;
    case DataDistributionType::Uniform:
      right_config = ColumnDataDistribution::make_uniform_config(0.0, 10000);
  }

  // TODO(anyone): replace with EncodingType::Dictionary once joins (especially index join) support these
  auto table_1 = table_generator->generate_table(std::vector<ColumnDataDistribution>{left_config}, state.range(0),
                                                 state.range(0) / 4, EncodingType::Dictionary);
  auto table_2 = table_generator->generate_table(std::vector<ColumnDataDistribution>{right_config}, state.range(1),
                                                 state.range(1) / 4, EncodingType::Dictionary);

  for (auto table : {table_1, table_2}) {
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

void BenchmarkJoinFixture::ChunkSizeInUni(benchmark::internal::Benchmark* b) {
  for (int left_size : {100, 1000, 10000, 100000, 1000000}) {
    for (int right_size : {100, 1000, 10000, 100000, 1000000, 5000000}) {
      // make sure we do not overrun our memory capacity
      if (static_cast<uint64_t>(left_size) * static_cast<uint64_t>(right_size) <= 1e9) {
        b->Args({left_size, right_size,
                 static_cast<int>(DataDistributionType::Uniform)});  // left size, right size, distribution
      }
    }
  }
}

void BenchmarkJoinFixture::ChunkSizeInNormal(benchmark::internal::Benchmark* b) {
  for (int left_size : {100, 1000, 10000, 100000, 1000000}) {
    for (int right_size : {100, 1000, 10000, 100000, 1000000, 5000000}) {
      // make sure we do not overrun our memory capacity
      if (static_cast<uint64_t>(left_size) * static_cast<uint64_t>(right_size) <= 1e9) {
        b->Args({left_size, right_size,
                 static_cast<int>(DataDistributionType::NormalSkewed)});  // left size, right size, distribution
      }
    }
  }
}
void BenchmarkJoinFixture::ChunkSizeInPareto(benchmark::internal::Benchmark* b) {
  for (int left_size : {100, 1000, 10000, 100000, 1000000}) {
    for (int right_size : {100, 1000, 10000, 100000, 1000000, 5000000}) {
      // make sure we do not overrun our memory capacity
      if (static_cast<uint64_t>(left_size) * static_cast<uint64_t>(right_size) <= 1e9) {
        b->Args({left_size, right_size,
                 static_cast<int>(DataDistributionType::Pareto)});  // left size, right size, distribution
      }
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
