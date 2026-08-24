#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_dyod.hpp"
#include "operators/aggregate_hash.hpp"
#include "operators/table_wrapper.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/encoding_type.hpp"
#include "storage/pos_lists/row_id_pos_list.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;

namespace {

void encode(const std::shared_ptr<Table>& table, const EncodingType encoding_type) {
  // Encoding immutable chunks also creates the pruning statistics exercised by the MIN/MAX fast path.
  for (auto chunk_id = ChunkID{0}; chunk_id < table->chunk_count(); ++chunk_id) {
    const auto chunk = table->get_chunk(chunk_id);
    if (chunk->is_mutable()) {
      chunk->set_immutable();
    }
  }
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{encoding_type});
}

// The d values are multiples of 0.5, so per-group sums are exact regardless of fold order.
std::shared_ptr<Table> make_input_table(const size_t row_count, const ChunkOffset chunk_size = ChunkOffset{2048}) {
  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, true},
                                                  {"b", DataType::String, true},
                                                  {"c", DataType::Int, false},
                                                  {"d", DataType::Double, true},
                                                  {"e", DataType::String, false}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, chunk_size);

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

  return table;
}

std::shared_ptr<TableWrapper> wrap_input(const std::shared_ptr<const Table>& table) {
  const auto wrapper = std::make_shared<TableWrapper>(table);
  wrapper->never_clear_output();
  wrapper->execute();
  return wrapper;
}

std::shared_ptr<TableWrapper> make_input(const size_t row_count) {
  return wrap_input(make_input_table(row_count));
}

std::shared_ptr<Table> make_low_cardinality_table(const size_t row_count,
                                                  const ChunkOffset chunk_size = ChunkOffset{2048}) {
  const auto definitions = TableColumnDefinitions{{"flag", DataType::Int, false},
                                                  {"status", DataType::String, false},
                                                  {"val_i", DataType::Int, true},
                                                  {"val_d", DataType::Double, true}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, chunk_size);

  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto flag = AllTypeVariant{static_cast<int32_t>(row % 4)};
    const auto status = AllTypeVariant{pmr_string{"s" + std::to_string((row / 7) % 3)}};
    const auto val_i = row % 11 == 0 ? AllTypeVariant{NullValue{}} : AllTypeVariant{static_cast<int32_t>(row % 100)};
    const auto val_d =
        row % 7 == 0 ? AllTypeVariant{NullValue{}} : AllTypeVariant{static_cast<double>(row % 200) * 0.5};
    table->append({flag, status, val_i, val_d});
  }

  return table;
}

// Half the rows carry group value 0; the rest spread over ~30k values.
std::shared_ptr<Table> make_skewed_table(const size_t row_count) {
  const auto definitions = TableColumnDefinitions{
      {"g", DataType::Int, false}, {"tag", DataType::String, false}, {"val", DataType::Int, false}};
  const auto table = std::make_shared<Table>(definitions, TableType::Data, ChunkOffset{2048});

  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto g = AllTypeVariant{row % 2 == 0 ? int32_t{0} : static_cast<int32_t>(1 + row % 30011)};
    const auto tag = AllTypeVariant{pmr_string{"t" + std::to_string(row % 733)}};
    const auto val = AllTypeVariant{static_cast<int32_t>(row % 1009)};
    table->append({g, tag, val});
  }

  return table;
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

TEST_F(OperatorsAggregateDYODTest, AnyOnGroupByColumns) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input, {{ColumnID{0}, WindowFunction::Any}, {ColumnID{1}, WindowFunction::Any}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, CountDistinctManyGroups) {
  const auto input = make_input(120'000);
  compare_with_aggregate_hash(
      input, {{ColumnID{2}, WindowFunction::CountDistinct}, {ColumnID{3}, WindowFunction::CountDistinct}},
      {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, CountDistinctOnStrings) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(
      input, {{ColumnID{1}, WindowFunction::CountDistinct}, {ColumnID{4}, WindowFunction::CountDistinct}},
      {ColumnID{2}});
}

TEST_F(OperatorsAggregateDYODTest, CountDistinctWithoutGroupBy) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::CountDistinct},
                               {ColumnID{1}, WindowFunction::CountDistinct},
                               {ColumnID{3}, WindowFunction::CountDistinct}},
                              {});
}

TEST_F(OperatorsAggregateDYODTest, MinMaxWithoutGroupBy) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input, {{ColumnID{1}, WindowFunction::Min}, {ColumnID{1}, WindowFunction::Max}}, {});
}

TEST_F(OperatorsAggregateDYODTest, StringAggregatesWithoutGroupByOnReferenceInput) {
  const auto input = wrap_input(to_simple_reference_table(make_input_table(30'000)));
  compare_with_aggregate_hash(input,
                              {{ColumnID{1}, WindowFunction::Min},
                               {ColumnID{1}, WindowFunction::Max},
                               {ColumnID{1}, WindowFunction::CountDistinct},
                               {ColumnID{4}, WindowFunction::Min}},
                              {});
}

TEST_F(OperatorsAggregateDYODTest, DictionaryMaxMinWithoutGroupBy) {
  const auto table = make_input_table(30'000);
  encode(table, EncodingType::Dictionary);
  compare_with_aggregate_hash(wrap_input(table),
                              {{ColumnID{1}, WindowFunction::Max}, {ColumnID{1}, WindowFunction::Min}}, {});
}

TEST_F(OperatorsAggregateDYODTest, UnencodedPruningStatisticsMinMaxWithoutGroupBy) {
  const auto table = make_input_table(30'000);
  encode(table, EncodingType::Unencoded);
  compare_with_aggregate_hash(wrap_input(table),
                              {{ColumnID{0}, WindowFunction::Min},
                               {ColumnID{0}, WindowFunction::Max},
                               {ColumnID{1}, WindowFunction::Min},
                               {ColumnID{1}, WindowFunction::Max}},
                              {});
}

TEST_F(OperatorsAggregateDYODTest, DictionaryMinMaxWithoutGroupByOnAllNullColumn) {
  const auto table =
      std::make_shared<Table>(TableColumnDefinitions{{"a", DataType::Int, true}}, TableType::Data, ChunkOffset{2});
  for (auto row = size_t{0}; row < 5; ++row) {
    table->append({NullValue{}});
  }
  encode(table, EncodingType::Dictionary);

  compare_with_aggregate_hash(wrap_input(table),
                              {{ColumnID{0}, WindowFunction::Min}, {ColumnID{0}, WindowFunction::Max}}, {});
}

TEST_F(OperatorsAggregateDYODTest, ReferenceInputSpanningChunks) {
  const auto input = wrap_input(to_simple_reference_table(make_input_table(30'000)));
  compare_with_aggregate_hash(
      input,
      {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{4}, WindowFunction::Min}, {ColumnID{0}, WindowFunction::Any}},
      {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, NullRowIdsInPosList) {
  const auto data = make_input_table(5'000);
  auto pos_list = std::make_shared<RowIDPosList>();
  for (auto chunk_id = ChunkID{0}; chunk_id < data->chunk_count(); ++chunk_id) {
    const auto chunk_size = data->get_chunk(chunk_id)->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      pos_list->emplace_back(pos_list->size() % 10 == 9 ? NULL_ROW_ID : RowID{chunk_id, chunk_offset});
    }
  }

  auto definitions = TableColumnDefinitions{};
  auto segments = Segments{};
  const auto column_count = data->column_count();
  for (auto column_id = ColumnID{0}; column_id < column_count; ++column_id) {
    definitions.emplace_back(data->column_name(column_id), data->column_data_type(column_id), true);
    segments.emplace_back(std::make_shared<ReferenceSegment>(data, column_id, pos_list));
  }
  const auto reference_table = std::make_shared<Table>(definitions, TableType::References);
  reference_table->append_chunk(segments);

  compare_with_aggregate_hash(
      wrap_input(reference_table),
      {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{4}, WindowFunction::Min}, {ColumnID{0}, WindowFunction::Any}},
      {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, DictionaryEncodedInput) {
  const auto table = make_input_table(30'000);
  table->last_chunk()->set_immutable();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
  compare_with_aggregate_hash(
      wrap_input(table),
      {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{4}, WindowFunction::Min}, {ColumnID{3}, WindowFunction::Avg}},
      {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, EmptyInput) {
  const auto input = make_input(0);
  compare_with_aggregate_hash(input, {{ColumnID{2}, WindowFunction::Sum}, {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, RejectsStddevSamp) {
  const auto input = make_input(64);
  const auto table = input->get_output();
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{standard_deviation_sample_(
      pqp_column_(ColumnID{2}, table->column_data_type(ColumnID{2}), table->column_name(ColumnID{2})))};
  const auto aggregate_dyod = std::make_shared<AggregateDYOD>(input, aggregates, std::vector<ColumnID>{ColumnID{0}});
  EXPECT_THROW(aggregate_dyod->execute(), std::logic_error);
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathNumericGroupBy) {
  const auto input = wrap_input(make_low_cardinality_table(100'000));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {ColumnID{3}, WindowFunction::Count},
                               {ColumnID{2}, WindowFunction::Min},
                               {ColumnID{2}, WindowFunction::Max},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathStringGroupBy) {
  const auto input = wrap_input(make_low_cardinality_table(100'000));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathMixedGroupBy) {
  const auto input = wrap_input(make_low_cardinality_table(100'000));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{2}, WindowFunction::Min},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathDictionaryEncoded) {
  const auto table = make_low_cardinality_table(100'000);
  table->last_chunk()->set_immutable();
  ChunkEncoder::encode_all_chunks(table, SegmentEncodingSpec{EncodingType::Dictionary});
  compare_with_aggregate_hash(
      wrap_input(table),
      {{ColumnID{3}, WindowFunction::Sum}, {ColumnID{3}, WindowFunction::Avg}, {ColumnID{2}, WindowFunction::Max}},
      {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathReferenceInput) {
  const auto input = wrap_input(to_simple_reference_table(make_low_cardinality_table(100'000)));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{2}, WindowFunction::Min},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathSingleChunkNoCombine) {
  const auto input = wrap_input(make_low_cardinality_table(1500));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {ColumnID{2}, WindowFunction::Min},
                               {ColumnID{2}, WindowFunction::Max},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, LowCardinalityPathStringValues) {
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{1}, WindowFunction::Min},
                               {ColumnID{1}, WindowFunction::Max},
                               {ColumnID{1}, WindowFunction::Count},
                               {ColumnID{4}, WindowFunction::Min},
                               {ColumnID{3}, WindowFunction::Sum}},
                              {ColumnID{2}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedLowCardinalityCombine) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{1}, WindowFunction::Min},
                               {ColumnID{1}, WindowFunction::Max},
                               {ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{3}, WindowFunction::Avg},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{2}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedCountDistinctWithoutGroupBy) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = make_input(30'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::CountDistinct},
                               {ColumnID{1}, WindowFunction::CountDistinct},
                               {ColumnID{3}, WindowFunction::CountDistinct}},
                              {});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedManyGroups) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = make_input(120'000);
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::Sum},
                               {ColumnID{4}, WindowFunction::Min},
                               {ColumnID{3}, WindowFunction::CountDistinct}},
                              {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedFewChunksManyGroups) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = wrap_input(make_input_table(120'000, ChunkOffset{65'535}));
  compare_with_aggregate_hash(input,
                              {{ColumnID{2}, WindowFunction::Sum},
                               {ColumnID{4}, WindowFunction::Min},
                               {ColumnID{3}, WindowFunction::Avg},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedFewChunksLowCardinality) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = wrap_input(make_low_cardinality_table(120'000, ChunkOffset{65'535}));
  compare_with_aggregate_hash(input,
                              {{ColumnID{3}, WindowFunction::Sum},
                               {ColumnID{2}, WindowFunction::Min},
                               {INVALID_COLUMN_ID, WindowFunction::Count}},
                              {ColumnID{0}, ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedSingleChunkLowCardinality) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = wrap_input(make_low_cardinality_table(50'000, ChunkOffset{65'535}));
  compare_with_aggregate_hash(
      input, {{ColumnID{3}, WindowFunction::Avg}, {ColumnID{2}, WindowFunction::Max}}, {ColumnID{1}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedSkewedGroupBy) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = wrap_input(make_skewed_table(150'000));
  compare_with_aggregate_hash(
      input,
      {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{1}, WindowFunction::Min}, {ColumnID{0}, WindowFunction::Any}},
      {ColumnID{0}});
}

TEST_F(OperatorsAggregateDYODTest, MultiThreadedSkewedCountDistinct) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());
  const auto input = wrap_input(make_skewed_table(150'000));
  compare_with_aggregate_hash(input, {{ColumnID{2}, WindowFunction::Sum}, {ColumnID{2}, WindowFunction::CountDistinct}},
                              {ColumnID{0}});
}

}  // namespace hyrise
