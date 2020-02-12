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

#include "micro_benchmark_utils.hpp"

namespace opossum {

static std::shared_ptr<Table> generate_custom_table(const size_t row_count, const ChunkOffset chunk_size,
                                                    const DataType data_type = DataType::Int,
                                                    const std::optional<float> null_ratio = std::nullopt) {
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  size_t num_columns = 2;

  const int max_different_value = 10'000;
  const std::vector<DataType> column_data_types = {num_columns, data_type};

  std::vector<ColumnSpecification> column_specifications = std::vector(
      num_columns, ColumnSpecification(ColumnDataDistribution::make_uniform_config(0.0, max_different_value), data_type,
                                       {EncodingType::Unencoded}, std::nullopt, null_ratio));

  return table_generator->generate_table(column_specifications, row_count, chunk_size, UseMvcc::Yes);
}

static void make_reference_table(std::shared_ptr<TableWrapper> table_wrapper) {
  auto limit =
      std::make_shared<Limit>(table_wrapper, expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
  limit->execute();
  table_wrapper = std::make_shared<TableWrapper>(limit->get_output());
  table_wrapper->execute();
}

static void BM_Sort(benchmark::State& state, const size_t row_count = 40'000, const ChunkOffset chunk_size = 2'000,
                    const DataType data_type = DataType::Int, const std::optional<float> null_ratio = std::nullopt,
                    const bool multi_column_sort = true, const bool use_reference_segment = false) {
  micro_benchmark_clear_cache();

  auto table_wrapper =
      std::make_shared<TableWrapper>(generate_custom_table(row_count, chunk_size, data_type, null_ratio));
  table_wrapper->execute();
  if (use_reference_segment) {
    make_reference_table(table_wrapper);
  }

  auto warm_up = std::make_shared<Sort>(
      table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}}, 2u);
  warm_up->execute();
  for (auto _ : state) {
    if (multi_column_sort) {
      std::vector<SortColumnDefinition> sort_definitions = {{ColumnID{0}, OrderByMode::Ascending},
                                                            {ColumnID{1}, OrderByMode::Descending}};
      auto sort = std::make_shared<Sort>(table_wrapper, sort_definitions, 2u);
      sort->execute();
    } else {
      auto sort = std::make_shared<Sort>(
          table_wrapper, std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}},
          2u);
      sort->execute();
    }
  }
}

static void BM_SortWithVaryingRowCount(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count);
}

static void BM_SortWithVaryingRowCountTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, ChunkOffset{2'000}, DataType::Int, std::nullopt, true);
}

static void BM_SortWithNullValues(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, ChunkOffset{2'000}, DataType::Int, std::optional<float>{0.2});
}

static void BM_SortWithReferenceSegments(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, ChunkOffset{2'000}, DataType::Int, std::nullopt, false, true);
}

static void BM_SortWithReferenceSegmentsTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, ChunkOffset{2'000}, DataType::Int, std::nullopt, true, true);
}

static void BM_SortWithStrings(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, ChunkOffset{2'000}, DataType::String);
}

BENCHMARK(BM_SortWithVaryingRowCount)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortWithVaryingRowCountTwoColumns)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortWithNullValues)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortWithReferenceSegments)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortWithReferenceSegmentsTwoColumns)->RangeMultiplier(10)->Range(4, 400'000);
BENCHMARK(BM_SortWithStrings)->RangeMultiplier(10)->Range(4, 400'000);

}  // namespace opossum
