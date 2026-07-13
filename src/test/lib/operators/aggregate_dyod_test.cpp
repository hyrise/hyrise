#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate_dyod.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;

namespace {

// The d values are multiples of 0.5, so per-group sums are exact regardless of fold order.
std::shared_ptr<TableWrapper> make_input(const size_t row_count) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                  {"b", DataType::String, true},
                                                  {"c", DataType::Int, false},
                                                  {"d", DataType::Double, true},
                                                  {"e", DataType::String, false}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, ChunkOffset{2048});

  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto a = row % 97 == 0 ? AllTypeVariant{NullValue{}} : AllTypeVariant{static_cast<int32_t>(row % 20011)};
    auto b = AllTypeVariant{NullValue{}};
    if (row % 13 != 0) {
      const auto group = row % 9001;
      b = row % 31 == 0
              ? AllTypeVariant{pmr_string{"group_with_a_key_longer_than_the_inline_blob_" + std::to_string(group)}}
              : AllTypeVariant{pmr_string{"g" + std::to_string(group)}};
    }
    const auto c = AllTypeVariant{static_cast<int32_t>(row % 1009)};
    const auto d = row % 7 == 0 ? AllTypeVariant{NullValue{}} : AllTypeVariant{static_cast<double>(row % 2003) * 0.5};
    const auto e = AllTypeVariant{pmr_string{"v" + std::to_string(row % 501)}};
    table->append({a, b, c, d, e});
  }

  const auto wrapper = std::make_shared<TableWrapper>(table);
  wrapper->never_clear_output();
  wrapper->execute();
  return wrapper;
}

void compare_with_aggregate_hash(const std::shared_ptr<AbstractOperator>& input,
                                 const std::vector<std::pair<ColumnID, WindowFunction>>& aggregate_definitions,
                                 const std::vector<ColumnID>& groupby_column_ids) {
  const auto table = input->get_output();
  auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{};
  aggregates.reserve(aggregate_definitions.size());
  for (const auto& [column_id, function] : aggregate_definitions) {
    if (column_id == INVALID_COLUMN_ID) {
      aggregates.emplace_back(
          std::make_shared<WindowFunctionExpression>(function, pqp_column_(column_id, DataType::Long, "*")));
    } else {
      aggregates.emplace_back(std::make_shared<WindowFunctionExpression>(
          function, pqp_column_(column_id, table->column_data_type(column_id), table->column_name(column_id))));
    }
  }

  const auto aggregate_dyod = std::make_shared<AggregateDYOD>(input, aggregates, groupby_column_ids);
  aggregate_dyod->execute();
  const auto aggregate_hash = std::make_shared<AggregateHash>(input, aggregates, groupby_column_ids);
  aggregate_hash->execute();

  EXPECT_TABLE_EQ_UNORDERED(aggregate_dyod->get_output(), aggregate_hash->get_output());
}

}  // namespace

class OperatorsAggregateDYODTest : public BaseTest {};

TEST_F(OperatorsAggregateDYODTest, NumericGroupByManyGroups) {
  const auto input = make_input(120'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {ColumnID{3}, WindowFunction::Count},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, NumericGroupByWideKey) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input, {{ColumnID{2}, WindowFunction::Min}, {ColumnID{3}, WindowFunction::Sum}},
                              {ColumnID{0}, ColumnID{2}, ColumnID{3}});
}

TEST_F(OperatorsAggregateDYODTest, StringGroupByWithNullsAndLongStrings) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{4}, WindowFunction::Min},
                               {ColumnID{4}, WindowFunction::Max},
                               {ColumnID{2}, WindowFunction::Sum},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, NullableStringValues) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(
      input,
      {{ColumnID{1}, WindowFunction::Min}, {ColumnID{1}, WindowFunction::Max}, {ColumnID{1}, WindowFunction::Count}},
      {ColumnID{2}});
}

TEST_F(OperatorsAggregateDYODTest, MixedGroupByWithNulls) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, EmptyInput) {
  const auto input = make_input(0);
  compare_with_aggregate_hash(input, {{ColumnID{2}, WindowFunction::Sum}, {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

}  // namespace hyrise
