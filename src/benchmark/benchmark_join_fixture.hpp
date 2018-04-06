#include <memory>

#include "benchmark/benchmark.h"
#include "types.hpp"

namespace opossum {

class TableWrapper;

// Defining the base fixture class
class BenchmarkJoinFixture : public benchmark::Fixture {
 public:
  void SetUp(::benchmark::State& state) override;
  void TearDown(::benchmark::State&) override;

  static void ChunkSizeInUni(benchmark::internal::Benchmark* b);
  static void ChunkSizeInPareto(benchmark::internal::Benchmark* b);
  static void ChunkSizeInNormal(benchmark::internal::Benchmark* b);

 protected:
  void clear_cache();

 protected:
  std::shared_ptr<TableWrapper> _tw_small_uni1, _tw_small_uni2;
  ChunkID _data_size;
};

}  // namespace opossum
