#include <memory>

#include "benchmark/benchmark.h"

#include "../micro_benchmark_basic_fixture.hpp"
#include "SQLParser.h"
#include "SQLParserResult.h"
#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "operators/limit.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "sql/sql_translator.hpp"
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

  void BM_SortSingleColumnSQL(benchmark::State& state) {
    const std::string query = R"(
        SELECT *
        FROM table_a
        ORDER BY col_1)";

    RunSQLBasedBenchmark(state, query);
  }

  void BM_SortMultiColumnSQL(benchmark::State& state) {
    const std::string query = R"(
        SELECT *
        FROM table_a
        ORDER BY col_1, col_2)";

    RunSQLBasedBenchmark(state, query);
  }

 protected:
  std::shared_ptr<Table> GenerateCustomTable(const size_t row_count, const ChunkOffset chunk_size,
                                             const DataType data_type = DataType::Int,
                                             const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                             const std::optional<float> null_ratio = std::nullopt) {
    const auto table_generator = std::make_shared<SyntheticTableGenerator>();

    size_t num_columns = 1;
    if (column_names.has_value()) {
      num_columns = column_names.value().size();
    }

    const int max_different_value = 10'000;
    const std::vector<DataType> column_data_types = {num_columns, data_type};

    return table_generator->generate_table(
        {num_columns, {ColumnDataDistribution::make_uniform_config(0.0, max_different_value)}}, column_data_types,
        row_count, chunk_size, std::vector<SegmentEncodingSpec>(num_columns, {EncodingType::Unencoded}), column_names,
        UseMvcc::Yes, null_ratio);
  }

  void InitializeCustomTableWrapper(const size_t row_count, const ChunkOffset chunk_size,
                                    const DataType data_type = DataType::Int,
                                    const std::optional<std::vector<std::string>>& column_names = std::nullopt,
                                    const std::optional<float> null_ratio = std::nullopt) {
    // We only set _table_wrapper_a because this is the only table (currently) used in the Sort Benchmarks
    _table_wrapper_a =
        std::make_shared<TableWrapper>(GenerateCustomTable(row_count, chunk_size, data_type, column_names, null_ratio));
    _table_wrapper_a->execute();
  }

  void MakeReferenceTable() {
    auto limit = std::make_shared<Limit>(_table_wrapper_a,
                                         expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
    limit->execute();
    _table_wrapper_a = std::make_shared<TableWrapper>(limit->get_output());
    _table_wrapper_a->execute();
  }

  void RunSQLBasedBenchmark(benchmark::State& state, const std::string& query) {
    _clear_cache();

    auto& storage_manager = Hyrise::get().storage_manager;
    auto column_names = std::optional<std::vector<std::string>>(1);
    column_names->push_back("col_1");
    column_names->push_back("col_2");
    storage_manager.add_table("table_a",
                              GenerateCustomTable(size_t{40'000}, ChunkOffset{2'000}, DataType::Int, column_names));

    for (auto _ : state) {
      hsql::SQLParserResult result;
      hsql::SQLParser::parseSQLString(query, &result);
      auto result_node = SQLTranslator{UseMvcc::No}.translate_parser_result(result)[0];
      const auto pqp = LQPTranslator{}.translate_node(result_node);
      const auto tasks = OperatorTask::make_tasks_from_operator(pqp, CleanupTemporaries::Yes);
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(tasks);
    }
  }
};

class SortPicoBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{2}, ChunkOffset{2'000}); }
};

class SortSmallBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{4'000}, ChunkOffset{2'000}); }
};

class SortLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override { InitializeCustomTableWrapper(size_t{400'000}, ChunkOffset{2'000}); }
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
    InitializeCustomTableWrapper(size_t{4'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortStringBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{40'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortStringLargeBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{400'000}, ChunkOffset{2'000}, DataType::String);
  }
};

class SortNullBenchmark : public SortBenchmark {
 public:
  void SetUp(benchmark::State& st) override {
    InitializeCustomTableWrapper(size_t{40'000}, ChunkOffset{2'000}, DataType::Int, std::nullopt,
                                 std::optional<float>{0.2});
  }
};

BENCHMARK_F(SortBenchmark, BM_Sort)(benchmark::State& state) { BM_Sort(state); }

BENCHMARK_F(SortBenchmark, BM_SortSingleColumnSQL)(benchmark::State& state) { BM_SortSingleColumnSQL(state); }

BENCHMARK_F(SortBenchmark, BM_SortMultiColumnSQL)(benchmark::State& state) { BM_SortMultiColumnSQL(state); }

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
