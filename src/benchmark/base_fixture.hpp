#include <memory>

#include "benchmark/benchmark.h"

#include "types.hpp"

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class BenchmarkBasicFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override;
  void TearDown(::benchmark::State&) override;

  static void ChunkSizeIn(benchmark::internal::Benchmark* b);

 protected:
  std::shared_ptr<TableWrapper> _table_wrapper_a;
  std::shared_ptr<TableWrapper> _table_wrapper_b;
  ChunkID _chunk_size;

  void clear_cache();
};

}  // namespace opossum
