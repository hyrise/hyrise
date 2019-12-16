#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "expression/expression_functional.hpp"
#include "operators/join_hash.hpp"
#include "operators/limit.hpp"
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
  void SetUpWithOverriddenSize(const size_t row_count, const ChunkOffset chunk_size,
                               const DataType data_type = DataType::Int) {
    const auto table_generator = std::make_shared<SyntheticTableGenerator>();
    const int num_columns = 1;
    const int max_different_value = 10'000;
    const std::vector<DataType> column_data_types = {num_columns, data_type};

    // We only set _table_wrapper_a because this is the only table (currently) used in the Sort Benchmarks
    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_table(
        {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value)}}, column_data_types,
        row_count, chunk_size, std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), std::nullopt,
        UseMvcc::No));
    _table_wrapper_a->execute();
  }

  void MakeReferenceTable() {
    auto limit = std::make_shared<Limit>(_table_wrapper_a,
                                         expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
    limit->execute();
    _table_wrapper_a = std::make_shared<TableWrapper>(limit->get_output());
    _table_wrapper_a->execute();
  }
};

class SortPicoBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { GenerateCustomTable(ChunkOffset{2'000}, size_t{2}); }
};

class SortSmallBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { GenerateCustomTable(ChunkOffset{2'000}, size_t{4'000}); }
};

class SortLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { GenerateCustomTable(ChunkOffset{2'000}, size_t{400'000}); }
};

class SortReferencePicoBenchmark : public SortPicoBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortPicoBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortReferenceSmallBenchmark : public SortSmallBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortSmallBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortReferenceBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortReferenceLargeBenchmark : public SortLargeBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    SortLargeBenchmark::SetUp(st);
    MakeReferenceTable();
  }
};

class SortStringSmallBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    GenerateCustomTable(ChunkOffset{2'000}, size_t{4'000}, DataType::String);
  }
};

class SortStringBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    GenerateCustomTable(ChunkOffset{2'000}, size_t{40'000}, DataType::String);
  }
};

class SortStringLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    GenerateCustomTable(ChunkOffset{2'000}, size_t{400'000}, DataType::String);
  }
};

class SortNullBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    const auto table_generator = std::make_shared<SyntheticTableGenerator>();
    const int num_columns = 1;
    const int max_different_value_a = 40'000;
    const int max_different_value_b = 20'000;
    const size_t chunk_size{2'000};
    const std::vector<DataType> column_data_types = {num_columns, DataType::Int};

    _table_wrapper_a = std::make_shared<TableWrapper>(table_generator->generate_table(
        {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value_a)}}, column_data_types,
        ChunkOffset{max_different_value_a}, chunk_size,
        std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), std::nullopt, UseMvcc::No));
    _table_wrapper_a->execute();

    _table_wrapper_b = std::make_shared<TableWrapper>(table_generator->generate_table(
        {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value_b)}}, column_data_types,
        ChunkOffset{max_different_value_b}, chunk_size,
        std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), std::nullopt, UseMvcc::No));
    _table_wrapper_b->execute();

    auto join = std::make_shared<JoinHash>(
        _table_wrapper_a, _table_wrapper_b, JoinMode::Left,
        OperatorJoinPredicate{ColumnIDPair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals});
    join->execute();

    // TODO(sorting-group): Find a more reliable way to produce an exct number of NULL values. Right now this produces
    //  ~20% NULL Values of the bigger table

    _table_wrapper_a = std::make_shared<TableWrapper>(join->get_output());
    _table_wrapper_a->execute();
  }
};

BENCHMARK_F(SortBenchmark, BM_Sort)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortPicoBenchmark, BM_SortPico)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortSmallBenchmark, BM_SortSmall)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortLargeBenchmark, BM_SortLarge)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortReferenceBenchmark, BM_SortReference)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortReferencePicoBenchmark, BM_SortReferencePico)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortReferenceSmallBenchmark, BM_SortReferenceSmall)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortReferenceLargeBenchmark, BM_SortReferenceLarge)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortStringBenchmark, BM_SortString)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortStringSmallBenchmark, BM_SortStringSmall)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortStringLargeBenchmark, BM_SortStringLarge)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortNullBenchmark, BM_SortNullBenchmark)(benchmark::State& state) { BM_Sort(state); }

}  // namespace opossum
