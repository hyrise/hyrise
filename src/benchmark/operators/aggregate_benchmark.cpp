#include <memory>
#include <vector>

#include "../micro_benchmark_basic_fixture.hpp"
#include "benchmark/benchmark.h"
#include "expression/expression_functional.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/aggregate_sort.hpp"
#include "operators/sort.hpp"
#include "operators/table_wrapper.hpp"
#include "types.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateHash)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{0} /* "a" */};

  auto warm_up = std::make_shared<AggregateHash>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateHash>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortNotSortedNoGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {};

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortSortedNoGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {};

  const auto sort =
      std::make_shared<Sort>(_table_wrapper_a, std::vector<SortColumnDefinition>{SortColumnDefinition(ColumnID{1})});
  sort->execute();

  auto table_wrapper_sorted = std::make_shared<TableWrapper>(sort->get_output());
  table_wrapper_sorted->execute();

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(table_wrapper_sorted, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortNotSortedOneGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{1}};

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortSortedOneGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{1}};

  const auto sort =
      std::make_shared<Sort>(_table_wrapper_a, std::vector<SortColumnDefinition>{SortColumnDefinition(ColumnID{1})});
  sort->execute();

  auto table_wrapper_sorted = std::make_shared<TableWrapper>(sort->get_output());
  table_wrapper_sorted->execute();

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(table_wrapper_sorted, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortNotSortedMultipleGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}};

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
    aggregate->execute();
  }
}

BENCHMARK_F(MicroBenchmarkBasicFixture, BM_AggregateSortSortedMultipleGroupBy)(benchmark::State& state) {
  _clear_cache();

  auto aggregates = std::vector<std::shared_ptr<AggregateExpression>>{
      std::static_pointer_cast<AggregateExpression>(min_(pqp_column_(ColumnID{1}, DataType::Int, false, "b")))};

  std::vector<ColumnID> groupby = {ColumnID{0}, ColumnID{1}};

  const auto sort =
      std::make_shared<Sort>(_table_wrapper_a, std::vector<SortColumnDefinition>{SortColumnDefinition(ColumnID{1})});
  sort->execute();

  auto table_wrapper_sorted = std::make_shared<TableWrapper>(sort->get_output());
  table_wrapper_sorted->execute();

  auto warm_up = std::make_shared<AggregateSort>(_table_wrapper_a, aggregates, groupby);
  warm_up->execute();
  for (auto _ : state) {
    auto aggregate = std::make_shared<AggregateSort>(table_wrapper_sorted, aggregates, groupby);
    aggregate->execute();
  }
}

}  // namespace opossum
