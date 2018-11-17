#include "micro_benchmark_basic_fixture.hpp"

#include <memory>
#include <vector>

#include "benchmark/benchmark.h"
#include "operators/table_wrapper.hpp"
#include "storage/chunk.hpp"
#include "storage/storage_manager.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace {
// Generating a table with 40,000 rows (see TableGenerator), a chunk size of 2,000 results in 20 chunks per table
constexpr auto CHUNK_SIZE = opossum::ChunkID{2000};
}  // namespace

namespace opossum {

void MicroBenchmarkBasicFixture::SetUp(::benchmark::State& state) {
  auto chunk_size = ChunkID(CHUNK_SIZE);

  auto table_generator = std::make_shared<TableGenerator>();
  auto table_generator2 = std::make_shared<TableGenerator>();

  _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_table(chunk_size));
  _table_wrapper_b = std::make_shared<TableWrapper>(table_generator2->generate_table(chunk_size));
  _table_dict_wrapper =
      std::make_shared<TableWrapper>(table_generator->generate_table(chunk_size, EncodingType::Dictionary));
  _table_wrapper_a->execute();
  _table_wrapper_b->execute();
  _table_dict_wrapper->execute();
}

void MicroBenchmarkBasicFixture::TearDown(::benchmark::State&) { opossum::StorageManager::get().reset(); }

void MicroBenchmarkBasicFixture::_clear_cache() {
  std::vector<int> clear = std::vector<int>();
  clear.resize(500 * 1000 * 1000, 42);
  for (uint i = 0; i < clear.size(); i++) {
    clear[i] += 1;
  }
  clear.resize(0);
}

}  // namespace opossum
