#include <memory>
#include <vector>
#include <random>

#include "benchmark/benchmark.h"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_hashsort.hpp"
#include "operators/table_wrapper.hpp"
#include "operators/print.hpp"
#include "storage/value_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"

using namespace opossum;

namespace {

template<typename T>
std::shared_ptr<ValueSegment<std::enable_if_t<std::is_integral_v<T>, T>>> make_segment(const size_t row_count, const size_t distinct_count, const size_t seed) {
  std::uniform_int_distribution<T> dist{0, static_cast<T>(distinct_count - 1u)};
  std::bernoulli_distribution null_dist(0.2f);
  std::mt19937 gen(seed);

  std::vector<T> values(row_count);
  std::vector<bool> null_values(row_count);

  for (auto i = size_t{0}; i < row_count; ++i) {
    values[i] = dist(gen);
    null_values[i] = null_dist(gen);
  }

  return std::make_shared<ValueSegment<T>>(std::move(values), std::move(null_values));
}

template<typename T>
std::shared_ptr<ValueSegment<std::enable_if_t<std::is_floating_point_v<T>, T>>> make_segment(const size_t row_count, const size_t distinct_count, const size_t seed) {
  std::uniform_int_distribution<int32_t> dist{0, static_cast<int32_t>(distinct_count - 1u)};
  std::bernoulli_distribution null_dist(0.2f);
  std::mt19937 gen(seed);

  std::vector<T> values(row_count);
  std::vector<bool> null_values(row_count);

  for (auto i = size_t{0}; i < row_count; ++i) {
    values[i] = static_cast<T>(dist(gen)) / 10.0f;
    null_values[i] = null_dist(gen);
  }

  return std::make_shared<ValueSegment<T>>(std::move(values), std::move(null_values));
}

template<typename T>
std::shared_ptr<ValueSegment<std::enable_if_t<std::is_same_v<T, pmr_string>, T>>> make_segment(const size_t row_count, const size_t distinct_count, const size_t seed) {
  std::uniform_int_distribution<int32_t> string_length_dist{1, 12};
  std::uniform_int_distribution<char> char_dist{'A', 'z'};
  std::mt19937 gen(seed);

  std::vector<T> pool;
  for (auto i = size_t{0}; i < distinct_count; ++i) {
    const auto string_length = string_length_dist(gen);

    auto value = pmr_string(string_length, ' ');
    for (auto& c : value) {
      c = char_dist(gen);
    }

    pool.emplace_back(value);
  }

  std::uniform_int_distribution<size_t> pool_dist{0u, pool.size() - 1u};
  std::bernoulli_distribution null_dist(0.2f);

  std::vector<T> values(row_count);
  std::vector<bool> null_values(row_count);

  for (auto i = size_t{0}; i < row_count; ++i) {
    values[i] = pool[pool_dist(gen)];
    null_values[i] = null_dist(gen);
  }

  return std::make_shared<ValueSegment<T>>(std::move(values), std::move(null_values));
}

}

namespace opossum {

struct GroupByColumnDesc {
  DataType data_type;
  size_t distinct_count;
};

struct AggregateBenchmarkConfig {
  AggregateBenchmarkConfig(const size_t row_count, const std::vector<GroupByColumnDesc>& group_by_columns):
    row_count(row_count), group_by_columns(group_by_columns) {}

  size_t row_count;
  std::vector<GroupByColumnDesc> group_by_columns;
};

void BM_Aggregate(benchmark::State& state, const AggregateBenchmarkConfig& config) {
  auto column_definitions = TableColumnDefinitions{};
  auto column_id = ColumnID{0};
  auto segments = Segments{};
  auto group_by_column_ids = std::vector<ColumnID>{};

  for (const auto& group_by_column_desc : config.group_by_columns) {
    column_definitions.emplace_back("column_" + std::to_string(column_id), group_by_column_desc.data_type, true);
    resolve_data_type(group_by_column_desc.data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segments.emplace_back(make_segment<ColumnDataType>(config.row_count, group_by_column_desc.distinct_count, column_id));
    });
    group_by_column_ids.emplace_back(column_id);
    ++column_id;
  }

  const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
  table->append_chunk(std::move(segments));

  const auto table_op = std::make_shared<TableWrapper>(table);
  table_op->execute();

  auto aggregates = std::vector<AggregateColumnDefinition>{};

  auto row_count = size_t{0};
  auto chunk_count = size_t{0};
  for (auto _ : state) {
    const auto aggregate_op = std::make_shared<AggregateHashSort>(table_op, aggregates, group_by_column_ids);
    aggregate_op->execute();
    row_count = aggregate_op->get_output()->row_count();
    chunk_count = aggregate_op->get_output()->chunk_count();
  }

  std::cout << "Chunk count: " << chunk_count << std::endl;
}

// clang-format off
BENCHMARK_CAPTURE(BM_Aggregate, BM_10kR_9G_is, AggregateBenchmarkConfig(10'000, {{DataType::Int, 2}, {DataType::String, 2}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10kR_5kG_is, AggregateBenchmarkConfig(10'000, {{DataType::Int, 100}, {DataType::String, 100}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_5G_i, AggregateBenchmarkConfig(10'000'000, {{DataType::Int, 4}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_5kG_i, AggregateBenchmarkConfig(10'000'000, {{DataType::Int, 4'999}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_500kG_i, AggregateBenchmarkConfig(10'000'000, {{DataType::Int, 499'999}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_50mR_10mG_i, AggregateBenchmarkConfig(50'000'000, {{DataType::Int, 10'000'000}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_9G_li, AggregateBenchmarkConfig(10'000'000, {{DataType::Long, 2}, {DataType::Int, 2}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_5kG_li, AggregateBenchmarkConfig(10'000'000, {{DataType::Long, 70}, {DataType::Int, 70}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_500kG_li, AggregateBenchmarkConfig(10'000'000, {{DataType::Long, 707}, {DataType::Int, 707}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_10mR_500kG_ili, AggregateBenchmarkConfig(10'000'000, {{DataType::Int, 80}, {DataType::Long, 79}, {DataType::Int, 80}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_1mR_500kG_ilss, AggregateBenchmarkConfig(1'000'000, {{DataType::Int, 34}, {DataType::Long, 34}, {DataType::String, 35}, {DataType::String, 35}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_1mR_500kG_ss, AggregateBenchmarkConfig(1'000'000, {{DataType::String, 1'200}, {DataType::String, 1'200}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_1mR_500G_ss, AggregateBenchmarkConfig(1'000'000, {{DataType::String, 22}, {DataType::String, 22}}));
BENCHMARK_CAPTURE(BM_Aggregate, BM_20mR_4mG_is, AggregateBenchmarkConfig(20'000'000, {{DataType::Int, 2'100}, {DataType::String, 2'100}}));
// clang-format on

}  // namespace opossum
