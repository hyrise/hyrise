#include "base_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace opossum {

void BenchmarkBasicFixture::SetUp(::benchmark::State& state) {
  // Generating a test table with generate_table function from table_generator.cpp
  _chunk_size = static_cast<ChunkID>(state.range(0));

  auto table_generator = std::make_shared<TableGenerator>();

  auto table_generator2 = std::make_shared<TableGenerator>();

  _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->get_table(_chunk_size));
  _table_wrapper_b = std::make_shared<TableWrapper>(table_generator2->get_table(_chunk_size));
  _table_dict_wrapper = std::make_shared<TableWrapper>(table_generator->get_table(_chunk_size, true));
  _table_wrapper_a->execute();
  _table_wrapper_b->execute();
  _table_dict_wrapper->execute();
}

void BenchmarkBasicFixture::TearDown(::benchmark::State&) { opossum::StorageManager::get().reset(); }

void BenchmarkBasicFixture::ChunkSizeIn(benchmark::internal::Benchmark* b) {
  for (ChunkID i : {ChunkID(0), ChunkID(10000), ChunkID(100000)}) {
    b->Args({static_cast<int>(i)});  // i = chunk size
  }
}

void BenchmarkBasicFixture::clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}  // namespace opossum
