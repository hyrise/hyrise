#include <memory>
#include <numeric>
#include <vector>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/join_hash.hpp"
#include "operators/join_sort_merge.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace {

constexpr auto TABLE_SIZE = size_t{1'000};

}

namespace opossum {

using namespace opossum::expression_functional;  //NOLINT

// Generates a vector of zip codes with various numbers of representation
pmr_vector<int32_t> generate_zip_codes(const size_t table_size) {
  auto values = pmr_vector<int32_t>(table_size);

  int zip_count = 1;
  int zip_code = 10000;
  size_t total_entries = 0;
  bool finished = false;

  while (!finished) {
    for (int zip_counter = 0; zip_counter < zip_count; ++zip_counter) {
      if (total_entries <= table_size) {
        values[total_entries] = zip_code;
        total_entries++;
      } else {
        finished = true;
        break;
      }
    }
    zip_count *= 2;
    zip_code += 250;
  }

  return values;
}

pmr_vector<int32_t> generate_ages(const size_t table_size) {
  auto values = pmr_vector<int32_t>(table_size);
  for (size_t row_index = 0; row_index < table_size; ++row_index) {
    values[row_index] = rand_r() % 100 + 1;
  }

  return values;
}

std::shared_ptr<Table> create_table(const size_t table_size, pmr_vector<int32_t> values) {
  auto table_column_definitions = opossum::TableColumnDefinitions();
  table_column_definitions.emplace_back("a", DataType::Int, false);
  table_column_definitions.emplace_back("b", DataType::Int, false);

  auto ids_vector = pmr_vector<int32_t>(table_size);
  std::iota(ids_vector.begin(), ids_vector.end(), 1);

  const auto ids_value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(ids_vector));

  std::shared_ptr<Table> table;
  table = std::make_shared<Table>(table_column_definitions, TableType::Data);
  const auto value_segment = std::make_shared<ValueSegment<int32_t>>(std::move(values));
  Segments segments;
  segments.push_back(ids_value_segment);
  segments.push_back(value_segment);

  table->append_chunk({segments});
  // zip_table->append_chunk({ids_value_segment});

  return table;
}

std::shared_ptr<TableWrapper> create_zip_table(const size_t table_size) {
  auto zip_values = generate_zip_codes(table_size);

  auto zip_table = create_table(table_size, zip_values);

  const auto chunk_count = zip_table->chunk_count();
  for (ChunkID chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
    auto chunk = zip_table->get_chunk(chunk_index);
    chunk->finalize();
    chunk->set_ordered_by(std::make_pair(ColumnID{0}, OrderByMode::Ascending));
    chunk->set_ordered_by(std::make_pair(ColumnID{1}, OrderByMode::Ascending));
  }

  return std::make_shared<TableWrapper>(zip_table);
}

std::shared_ptr<TableWrapper> create_ages_table(const size_t table_size) {
  auto ages_values = generate_ages(table_size);

  auto ages_table = create_table(table_size, ages_values);

  const auto chunk_count = ages_table->chunk_count();
  for (ChunkID chunk_index = ChunkID{0}; chunk_index < chunk_count; ++chunk_index) {
    auto chunk = ages_table->get_chunk(chunk_index);
    chunk->finalize();
    chunk->set_ordered_by(std::make_pair(ColumnID{0}, OrderByMode::Ascending));
  }

  return std::make_shared<TableWrapper>(ages_table);
}

template <typename AggType, typename JoinType>
void BM_Join_Aggregate(benchmark::State& state) {
  auto table_wrapper_left = create_ages_table(TABLE_SIZE);
  table_wrapper_left->execute();
  auto table_wrapper_right = create_zip_table(TABLE_SIZE);
  table_wrapper_right->execute();

  auto operator_join_predicate =
      OperatorJoinPredicate(std::make_pair(ColumnID{0}, ColumnID{0}), PredicateCondition::Equals);

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(avg_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{3}};

  auto join =
      std::make_shared<JoinType>(table_wrapper_left, table_wrapper_right, JoinMode::Inner, operator_join_predicate);
  join->execute();
  auto warm_up = std::make_shared<AggType>(join, aggregates, groupby);
  warm_up->execute();

  for (auto _ : state) {
    auto join =
        std::make_shared<JoinType>(table_wrapper_left, table_wrapper_right, JoinMode::Inner, operator_join_predicate);
    join->execute();
    auto aggregate_sort = std::make_shared<AggType>(join, aggregates, groupby);
    aggregate_sort->execute();
  }
}

BENCHMARK_TEMPLATE(BM_Join_Aggregate, AggregateSort, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_Aggregate, AggregateSort, JoinHash);
BENCHMARK_TEMPLATE(BM_Join_Aggregate, AggregateHash, JoinSortMerge);
BENCHMARK_TEMPLATE(BM_Join_Aggregate, AggregateHash, JoinHash);

}  // namespace opossum
