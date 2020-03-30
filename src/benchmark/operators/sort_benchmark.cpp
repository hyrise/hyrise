#include <memory>

#include "../micro_benchmark_basic_fixture.hpp"
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

static std::shared_ptr<Table> generate_custom_table(const size_t row_count, const DataType data_type = DataType::Int,
                                                    const float null_ratio = 0.0f) {
  const auto table_generator = std::make_shared<SyntheticTableGenerator>();

  constexpr auto NUM_COLUMNS = 2;
  constexpr auto LARGEST_VALUE = 10'000;

  const std::vector<DataType> column_data_types = {NUM_COLUMNS, data_type};

  std::vector<ColumnSpecification> column_specifications =
      std::vector(NUM_COLUMNS, ColumnSpecification(ColumnDataDistribution::make_uniform_config(0.0, LARGEST_VALUE),
                                                   data_type, {EncodingType::Unencoded}, std::nullopt, null_ratio));

  return table_generator->generate_table(column_specifications, row_count);
}

static void BM_Sort(benchmark::State& state, const size_t row_count = 40'000, const DataType data_type = DataType::Int,
                    const float null_ratio = 0.0f, const bool multi_column_sort = true,
                    const bool use_reference_segment = false) {
  micro_benchmark_clear_cache();

  const auto input_table = generate_custom_table(row_count, data_type, null_ratio);
  std::shared_ptr<AbstractOperator> input_operator = std::make_shared<TableWrapper>(input_table);
  input_operator->execute();
  if (use_reference_segment) {
    input_operator = std::make_shared<Limit>(input_operator,
                                             expression_functional::to_expression(std::numeric_limits<int64_t>::max()));
    input_operator->execute();
  }

  auto sort_definitions = std::vector<SortColumnDefinition>{};
  if (multi_column_sort) {
    sort_definitions = std::vector<SortColumnDefinition>{{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending},
                                                          SortColumnDefinition{ColumnID{1}, OrderByMode::Descending}}};
  } else {
    sort_definitions = std::vector<SortColumnDefinition>{SortColumnDefinition{ColumnID{0}, OrderByMode::Ascending}};
  }

  for (auto _ : state) {
    auto sort = std::make_shared<Sort>(input_operator, sort_definitions);
    sort->execute();
  }
}

static void BM_SortWithVaryingRowCount(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count);
}

static void BM_SortWithVaryingRowCountTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, true);
}

static void BM_SortWithNullValues(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.2f);
}

static void BM_SortWithReferenceSegments(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, false, true);
}

static void BM_SortWithReferenceSegmentsTwoColumns(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::Int, 0.0f, true, true);
}

static void BM_SortWithStrings(benchmark::State& state) {
  const size_t row_count = state.range(0);
  BM_Sort(state, row_count, DataType::String);
}

BENCHMARK(BM_SortWithVaryingRowCount)->RangeMultiplier(7)->Range(10, 1'000'000);
BENCHMARK(BM_SortWithVaryingRowCountTwoColumns)->RangeMultiplier(7)->Range(10, 1'000'000);
BENCHMARK(BM_SortWithNullValues)->RangeMultiplier(7)->Range(10, 1'000'000);
BENCHMARK(BM_SortWithReferenceSegments)->RangeMultiplier(7)->Range(10, 1'000'000);
BENCHMARK(BM_SortWithReferenceSegmentsTwoColumns)->RangeMultiplier(7)->Range(10, 1'000'000);
BENCHMARK(BM_SortWithStrings)->RangeMultiplier(7)->Range(10, 1'000'000);

}  // namespace opossum
