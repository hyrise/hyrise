#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"

#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "table_generator.hpp"
#include "types.hpp"

namespace opossum {

// Defining the base fixture class
class BenchmarkBasicFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override {
    // Generating a test table with generate_table function from table_generator.cpp

    auto table_generator = std::make_shared<TableGenerator>();

    auto table_generator2 = std::make_shared<TableGenerator>();

    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->get_table(static_cast<ChunkID>(state.range(0))));
    _table_wrapper_b =
        std::make_shared<TableWrapper>(table_generator2->get_table(static_cast<ChunkID>(state.range(0))));
    _table_wrapper_a->execute();
    _table_wrapper_b->execute();
  }

  void TearDown(::benchmark::State&) override { opossum::StorageManager::get().reset(); }

  static void ChunkSizeIn(benchmark::internal::Benchmark* b) {
    for (ChunkID i : {ChunkID(0), ChunkID(10000), ChunkID(100000)}) {
      b->Args({static_cast<int>(i)});  // i = chunk size
    }
  }

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;

  void clear_cache() {
    std::vector<int> clear = std::vector<int>();
    clear.resize(500 * 1000 * 1000, 42);
    for (uint i = 0; i < clear.size(); i++) {
      clear[i] += 1;
    }
    clear.resize(0);
  }
};
}  // namespace opossum
