#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "synthetic_table_generator.hpp"

namespace opossum {

class SortBenchmark : public MicroBenchmarkBasicFixture {
 public:
  void BM_Sort(benchmark::State& state) {
    _clear_cache();

    auto warm_up = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
    warm_up->execute();
    for (auto _ : state) {
      auto sort = std::make_shared<Sort>(_table_wrapper_a, ColumnID{0} /* "a" */, OrderByMode::Ascending);
      sort->execute();
    }
  }

 protected:
  void SetUpWithOverriddenSize(const ChunkOffset& chunk_size, const size_t& row_count) {
    const auto table_generator = std::make_shared<SyntheticTableGenerator>();

    // We only set _table_wrapper_a because this is the only table (currently) used in the Sort Benchmarks
    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_table(2ul, row_count, chunk_size));
    _table_wrapper_a->execute();
  }
};

class SortSmallBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { SetUpWithOverriddenSize(ChunkOffset{200}, size_t{4'000}); }
};

class SortLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { SetUpWithOverriddenSize(ChunkOffset{20'000}, size_t{400'000}); }
};

BENCHMARK_F(SortBenchmark, BM_Sort)(benchmark::State& state) { BM_Sort(state); }
BENCHMARK_F(SortSmallBenchmark, BM_SortSmall)(benchmark::State& state) { BM_Sort(state); }
BENCHMARK_F(SortLargeBenchmark, BM_SortLarge)(benchmark::State& state) { BM_Sort(state); }

}  // namespace opossum
