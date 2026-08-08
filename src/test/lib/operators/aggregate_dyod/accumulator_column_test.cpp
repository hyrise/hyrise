#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <stdexcept>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "null_value.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;

namespace {

std::shared_ptr<Table> make_input_table(const TableColumnDefinitions& definitions,
                                        const std::vector<std::vector<AllTypeVariant>>& rows) {
  auto table = std::make_shared<Table>(definitions, TableType::Data);
  for (const auto& row : rows) {
    table->append(row);
  }
  return table;
}

std::shared_ptr<WindowFunctionExpression> make_aggregate(const WindowFunction function, const Table& table,
                                                         const ColumnID column_id) {
  if (column_id == INVALID_COLUMN_ID) {
    return std::make_shared<WindowFunctionExpression>(function, pqp_column_(column_id, DataType::Long, "*"));
  }
  return std::make_shared<WindowFunctionExpression>(
      function, pqp_column_(column_id, table.column_data_type(column_id), table.column_name(column_id)));
}

template <typename T>
std::vector<std::byte> pack_values(const std::vector<T>& values) {
  auto bytes = std::vector<std::byte>(values.size() * sizeof(T));
  std::memcpy(bytes.data(), values.data(), bytes.size());
  return bytes;
}

std::vector<AllTypeVariant> finalize_slots(const AbstractAccumulatorColumn& column, const size_t first_slot,
                                           const size_t last_slot, const DataType result_type) {
  const auto definitions = TableColumnDefinitions{{"result", result_type, true}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  column.finalize_into(first_slot, last_slot, 0, output);
  output.seal_all();
  const auto segments = output.column(0).take_segments();

  auto values = std::vector<AllTypeVariant>{};
  const auto row_count = last_slot - first_slot;
  for (auto row = uint32_t{0}; row < row_count; ++row) {
    values.emplace_back((*segments[0])[ChunkOffset{row}]);
  }
  return values;
}

}  // namespace

class AggregateDYODAccumulatorColumnTest : public BaseTest {};

TEST_F(AggregateDYODAccumulatorColumnTest, BuildResolvesResultTypes) {
  const auto table = make_input_table(
      {{"a", DataType::Int, false}, {"b", DataType::Float, false}, {"c", DataType::String, false}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Avg, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Sum, *table, ColumnID{1}),
      make_aggregate(WindowFunction::Min, *table, ColumnID{1}),
      make_aggregate(WindowFunction::Max, *table, ColumnID{2}),
      make_aggregate(WindowFunction::Count, *table, ColumnID{2}),
      make_aggregate(WindowFunction::Count, *table, INVALID_COLUMN_ID),
      make_aggregate(WindowFunction::Any, *table, ColumnID{2}),
      make_aggregate(WindowFunction::CountDistinct, *table, ColumnID{2}),
  };
  const auto schema = AggregateSchema::build(aggregates, *table);

  EXPECT_EQ(schema.aggregate_count(), 9u);
  EXPECT_EQ(schema.result_type(0), DataType::Long);
  EXPECT_EQ(schema.result_type(1), DataType::Double);
  EXPECT_EQ(schema.result_type(2), DataType::Double);
  EXPECT_EQ(schema.result_type(3), DataType::Float);
  EXPECT_EQ(schema.result_type(4), DataType::String);
  EXPECT_EQ(schema.result_type(5), DataType::Long);
  EXPECT_EQ(schema.result_type(6), DataType::Long);
  EXPECT_EQ(schema.result_type(7), DataType::String);
  EXPECT_EQ(schema.result_type(8), DataType::Long);
}

TEST_F(AggregateDYODAccumulatorColumnTest, BuildSharesValueStreamsAcrossAggregates) {
  const auto table = make_input_table({{"a", DataType::Int, false}, {"b", DataType::Long, false}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Avg, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Min, *table, ColumnID{1}),
      make_aggregate(WindowFunction::Count, *table, INVALID_COLUMN_ID),
  };
  const auto schema = AggregateSchema::build(aggregates, *table);

  EXPECT_EQ(schema.value_stream_count(), 2u);
  EXPECT_EQ(schema.aggregate_value_stream(0), schema.aggregate_value_stream(1));
  EXPECT_NE(schema.aggregate_value_stream(0), schema.aggregate_value_stream(2));
  EXPECT_EQ(schema.aggregate_value_stream(3), AggregateSchema::NO_VALUE_STREAM);
}

TEST_F(AggregateDYODAccumulatorColumnTest, LowCardinalityEligibilityAdmitsStringSources) {
  const auto table = make_input_table({{"a", DataType::Int, false}, {"b", DataType::String, true}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Min, *table, ColumnID{1}),
      make_aggregate(WindowFunction::Count, *table, ColumnID{1}),
  };
  EXPECT_TRUE(low_cardinality_eligible(AggregateSchema::build(aggregates, *table)));
}

TEST_F(AggregateDYODAccumulatorColumnTest, LowCardinalityEligibilityRejectsCountDistinctAndAny) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto count_distinct = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::CountDistinct, *table, ColumnID{0})};
  EXPECT_FALSE(low_cardinality_eligible(AggregateSchema::build(count_distinct, *table)));

  const auto any = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Any, *table, ColumnID{0})};
  EXPECT_FALSE(low_cardinality_eligible(AggregateSchema::build(any, *table)));
}

TEST_F(AggregateDYODAccumulatorColumnTest, BuildRejectsOutOfScopeFunctions) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::StandardDeviationSample, *table, ColumnID{0})};
  EXPECT_THROW(AggregateSchema::build(aggregates, *table), std::logic_error);

  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window =
      window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));
  const auto rank = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      std::make_shared<WindowFunctionExpression>(WindowFunction::Rank, nullptr, window)};
  EXPECT_THROW(AggregateSchema::build(rank, *table), std::logic_error);
}

TEST_F(AggregateDYODAccumulatorColumnTest, BuildRejectsInvalidTypeCombinations) {
  const auto table = make_input_table({{"a", DataType::String, false}}, {});
  for (const auto function : {WindowFunction::Sum, WindowFunction::Avg}) {
    const auto aggregates =
        std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(function, *table, ColumnID{0})};
    EXPECT_THROW(AggregateSchema::build(aggregates, *table), std::logic_error);
  }
}

TEST_F(AggregateDYODAccumulatorColumnTest, ValueNullBitmapWidthCountsNullableStreams) {
  const auto table = make_input_table({{"a", DataType::Int, false}, {"b", DataType::Int, true}}, {});

  const auto non_nullable =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  EXPECT_EQ(AggregateSchema::build(non_nullable, *table).value_null_bitmap_width(), 0u);

  const auto nullable = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Sum, *table, ColumnID{1}),
      make_aggregate(WindowFunction::Min, *table, ColumnID{1}),
  };
  EXPECT_EQ(AggregateSchema::build(nullable, *table).value_null_bitmap_width(), 1u);
}

TEST_F(AggregateDYODAccumulatorColumnTest, StringStreamHasFixedReferenceWidth) {
  const auto column = StringValueScatterColumn{ColumnID{0}, false};
  EXPECT_EQ(column.element_width(), 16u);
  EXPECT_FALSE(column.is_nullable());
  EXPECT_TRUE((StringValueScatterColumn{ColumnID{0}, true}).is_nullable());
}

TEST_F(AggregateDYODAccumulatorColumnTest, NumericStreamScattersRowsToTheirPartitions) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {{11}, {NullValue{}}, {22}, {33}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto column = NumericValueScatterColumn<int32_t>{ColumnID{0}, true};

  const auto value_stream_widths = std::vector<size_t>{sizeof(int32_t)};
  auto store = ScatterStore{PartitionCount{2}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/1,
                            /*needs_value_arena=*/false};
  const auto stream_widths = std::vector<size_t>{4, sizeof(int32_t), 1};
  auto heads = ScatterHeads{PartitionCount{2}, stream_widths.size(), stream_widths, true};
  const auto partitions = std::vector<PartitionId>{0, 1, 0, 1};
  auto bitmap = std::vector<std::byte>(4, std::byte{0});

  column.scatter(*segment, 0, 4, partitions, 1, heads, store, bitmap.data(), 1, 0);
  heads.finish(store);

  const auto& region_0 = store.value_region(0, 0);
  ASSERT_EQ(region_0.size(), 2 * sizeof(int32_t));
  auto values_0 = std::vector<int32_t>(2);
  std::memcpy(values_0.data(), region_0.data(), region_0.size());
  EXPECT_EQ(values_0[0], 11);
  EXPECT_EQ(values_0[1], 22);

  const auto& region_1 = store.value_region(1, 0);
  ASSERT_EQ(region_1.size(), 2 * sizeof(int32_t));
  auto values_1 = std::vector<int32_t>(2);
  std::memcpy(values_1.data(), region_1.data(), region_1.size());
  EXPECT_EQ(values_1[0], 0);
  EXPECT_EQ(values_1[1], 33);

  EXPECT_EQ(bitmap[0], std::byte{0});
  EXPECT_EQ(bitmap[1], std::byte{1});
  EXPECT_EQ(bitmap[2], std::byte{0});
  EXPECT_EQ(bitmap[3], std::byte{0});
}

TEST_F(AggregateDYODAccumulatorColumnTest, StringStreamScatterFillsPartitionArenas) {
  const auto table =
      make_input_table({{"a", DataType::String, true}},
                       {{pmr_string{"pear"}}, {NullValue{}}, {pmr_string{"apple"}}, {pmr_string{"fig"}}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Min, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);
  const auto& stream = schema.value_stream(0);

  const auto value_stream_widths = std::vector<size_t>{stream.element_width()};
  auto store = ScatterStore{PartitionCount{2}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/1,
                            /*needs_value_arena=*/true};
  const auto stream_widths = std::vector<size_t>{4, stream.element_width(), 1};
  auto heads = ScatterHeads{PartitionCount{2}, stream_widths.size(), stream_widths, true};
  const auto partitions = std::vector<PartitionId>{0, 0, 0, 1};
  auto bitmap = std::vector<std::byte>(4, std::byte{0});

  stream.scatter(*segment, 0, 4, partitions, 1, heads, store, bitmap.data(), 1, 0);
  heads.finish(store);
  EXPECT_EQ(bitmap[1], std::byte{1});

  const auto columns = schema.make_accumulator_columns();
  auto& min_column = *columns[0];

  const auto& region_0 = store.value_region(0, 0);
  ASSERT_EQ(region_0.size(), 3 * stream.element_width());
  min_column.grow_to(1);
  min_column.fold(std::vector<uint32_t>{0, 0, 0}, {region_0.data(), region_0.size()},
                  std::vector<std::byte>{std::byte{0b010}});
  EXPECT_EQ(finalize_slots(min_column, 0, 1, DataType::String)[0], AllTypeVariant{pmr_string{"apple"}});

  min_column.clear();
  const auto& region_1 = store.value_region(1, 0);
  ASSERT_EQ(region_1.size(), stream.element_width());
  min_column.grow_to(1);
  min_column.fold(std::vector<uint32_t>{0}, {region_1.data(), region_1.size()}, {});
  EXPECT_EQ(finalize_slots(min_column, 0, 1, DataType::String)[0], AllTypeVariant{pmr_string{"fig"}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, NumericStreamScattersOnlyItsRowWindow) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {{11}, {NullValue{}}, {22}, {33}, {44}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto column = NumericValueScatterColumn<int32_t>{ColumnID{0}, true};

  const auto value_stream_widths = std::vector<size_t>{sizeof(int32_t)};
  auto store = ScatterStore{PartitionCount{2}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/1,
                            /*needs_value_arena=*/false};
  const auto stream_widths = std::vector<size_t>{4, sizeof(int32_t), 1};
  auto heads = ScatterHeads{PartitionCount{2}, stream_widths.size(), stream_widths, true};
  const auto partitions = std::vector<PartitionId>{0, 1, 0};
  auto bitmap = std::vector<std::byte>(3, std::byte{0});

  column.scatter(*segment, 1, 4, partitions, 1, heads, store, bitmap.data(), 1, 0);
  heads.finish(store);

  const auto& region_0 = store.value_region(0, 0);
  ASSERT_EQ(region_0.size(), 2 * sizeof(int32_t));
  auto values_0 = std::vector<int32_t>(2);
  std::memcpy(values_0.data(), region_0.data(), region_0.size());
  EXPECT_EQ(values_0[0], 0);
  EXPECT_EQ(values_0[1], 33);

  const auto& region_1 = store.value_region(1, 0);
  ASSERT_EQ(region_1.size(), sizeof(int32_t));
  auto value_1 = int32_t{};
  std::memcpy(&value_1, region_1.data(), region_1.size());
  EXPECT_EQ(value_1, 22);

  EXPECT_EQ(bitmap[0], std::byte{1});
  EXPECT_EQ(bitmap[1], std::byte{0});
  EXPECT_EQ(bitmap[2], std::byte{0});
}

TEST_F(AggregateDYODAccumulatorColumnTest, StringStreamScattersOnlyItsRowWindow) {
  const auto table = make_input_table(
      {{"a", DataType::String, false}},
      {{pmr_string{"pear"}}, {pmr_string{"apple"}}, {pmr_string{"fig"}}, {pmr_string{"plum"}}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Min, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);
  const auto& stream = schema.value_stream(0);

  const auto value_stream_widths = std::vector<size_t>{stream.element_width()};
  auto store = ScatterStore{PartitionCount{2}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/0,
                            /*needs_value_arena=*/true};
  const auto stream_widths = std::vector<size_t>{4, stream.element_width()};
  auto heads = ScatterHeads{PartitionCount{2}, stream_widths.size(), stream_widths, false};
  const auto partitions = std::vector<PartitionId>{0, 0};

  stream.scatter(*segment, 1, 3, partitions, 1, heads, store, nullptr, 0, 0);
  heads.finish(store);

  const auto columns = schema.make_accumulator_columns();
  auto& min_column = *columns[0];
  const auto& region = store.value_region(0, 0);
  ASSERT_EQ(region.size(), 2 * stream.element_width());
  min_column.grow_to(1);
  min_column.fold(std::vector<uint32_t>{0, 0}, {region.data(), region.size()}, {});
  EXPECT_EQ(finalize_slots(min_column, 0, 1, DataType::String)[0], AllTypeVariant{pmr_string{"apple"}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, NeedsValueArenaOnlyForStringStreams) {
  const auto table = make_input_table({{"a", DataType::Int, false}, {"b", DataType::String, false}}, {});

  const auto numeric = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *table, ColumnID{0}),
      make_aggregate(WindowFunction::Count, *table, INVALID_COLUMN_ID),
  };
  EXPECT_FALSE(AggregateSchema::build(numeric, *table).needs_value_arena());

  const auto with_string =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Min, *table, ColumnID{1})};
  EXPECT_TRUE(AggregateSchema::build(with_string, *table).needs_value_arena());

  const auto any_string =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Any, *table, ColumnID{1})};
  EXPECT_FALSE(AggregateSchema::build(any_string, *table).needs_value_arena());
}

TEST_F(AggregateDYODAccumulatorColumnTest, SumFoldsPerSlotAndEmitsNullForEmptyGroups) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);
  const auto columns = schema.make_accumulator_columns();
  ASSERT_EQ(columns.size(), 1u);
  auto& column = *columns[0];

  column.grow_to(3);
  const auto slots = std::vector<uint32_t>{0, 1, 0, 1, 0};
  column.fold(slots, pack_values<int32_t>({1, 2, 3, 4, 5}), {});

  const auto results = finalize_slots(column, 0, 3, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{9}});
  EXPECT_EQ(results[1], AllTypeVariant{int64_t{6}});
  EXPECT_TRUE(variant_is_null(results[2]));
}

TEST_F(AggregateDYODAccumulatorColumnTest, FoldSkipsRowsWithSetNullBits) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(1);
  const auto slots = std::vector<uint32_t>{0, 0, 0};
  const auto null_bitmap = std::vector<std::byte>{std::byte{0b010}};
  column.fold(slots, pack_values<int32_t>({10, 999, 1}), null_bitmap);

  const auto results = finalize_slots(column, 0, 1, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{11}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, CountStarCountsEveryRow) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *table, INVALID_COLUMN_ID)};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(3);
  const auto slots = std::vector<uint32_t>{0, 1, 0};
  column.fold(slots, {}, {});

  const auto results = finalize_slots(column, 0, 3, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{2}});
  EXPECT_EQ(results[1], AllTypeVariant{int64_t{1}});
  EXPECT_EQ(results[2], AllTypeVariant{int64_t{0}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, CountColumnSkipsNullBits) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(1);
  const auto slots = std::vector<uint32_t>{0, 0};
  const auto null_bitmap = std::vector<std::byte>{std::byte{0b01}};
  column.fold(slots, pack_values<int32_t>({5, 7}), null_bitmap);

  const auto results = finalize_slots(column, 0, 1, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{1}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, AvgDividesBySeenCount) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Avg, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(3);
  const auto slots = std::vector<uint32_t>{0, 0, 1};
  column.fold(slots, pack_values<int32_t>({1, 2, 10}), {});

  const auto results = finalize_slots(column, 0, 3, DataType::Double);
  EXPECT_EQ(results[0], AllTypeVariant{1.5});
  EXPECT_EQ(results[1], AllTypeVariant{10.0});
  EXPECT_TRUE(variant_is_null(results[2]));
}

TEST_F(AggregateDYODAccumulatorColumnTest, AnyUsesNoValueStream) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Any, *table, ColumnID{0}),
                                                             make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);

  EXPECT_TRUE(schema.needs_row_id_stream());
  EXPECT_EQ(schema.value_stream_count(), 1u);
  EXPECT_EQ(schema.aggregate_value_stream(0), AggregateSchema::NO_VALUE_STREAM);
  EXPECT_EQ(schema.aggregate_value_stream(1), 0u);
}

TEST_F(AggregateDYODAccumulatorColumnTest, AnyGathersRepresentativeRows) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {{42}, {NullValue{}}, {7}});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Any, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(2);
  const auto slots = std::vector<uint32_t>{0, 1, 0};
  const auto row_ids = std::vector<RowID>{RowID{ChunkID{0}, ChunkOffset{0}}, RowID{ChunkID{0}, ChunkOffset{1}},
                                          RowID{ChunkID{0}, ChunkOffset{2}}};
  column.fold(slots, pack_values<RowID>(row_ids), {});

  const auto results = finalize_slots(column, 0, 2, DataType::Int);
  EXPECT_EQ(results[0], AllTypeVariant{42});
  EXPECT_TRUE(variant_is_null(results[1]));
}

TEST_F(AggregateDYODAccumulatorColumnTest, CountDistinctCountsDistinctNonNullValues) {
  const auto table = make_input_table({{"a", DataType::Int, true}}, {});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::CountDistinct, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(3);
  const auto slots = std::vector<uint32_t>{0, 0, 0, 1, 1, 0};
  const auto null_bitmap = std::vector<std::byte>{std::byte{0b010000}};
  column.fold(slots, pack_values<int32_t>({5, 5, 7, 5, 999, 5}), null_bitmap);

  const auto results = finalize_slots(column, 0, 3, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{2}});
  EXPECT_EQ(results[1], AllTypeVariant{int64_t{1}});
  EXPECT_EQ(results[2], AllTypeVariant{int64_t{0}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, CountDistinctDedupesStringsAcrossTiles) {
  const auto table =
      make_input_table({{"a", DataType::String, true}},
                       {{pmr_string{"pear"}}, {pmr_string{"apple"}}, {NullValue{}}, {pmr_string{"pear"}}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::CountDistinct, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);
  const auto columns = schema.make_accumulator_columns();
  auto& column = *columns[0];

  const auto& stream = schema.value_stream(0);
  const auto value_stream_widths = std::vector<size_t>{stream.element_width()};
  auto store = ScatterStore{PartitionCount{1}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/1,
                            /*needs_value_arena=*/true};
  const auto stream_widths = std::vector<size_t>{4, stream.element_width(), 1};
  auto heads = ScatterHeads{PartitionCount{1}, stream_widths.size(), stream_widths, true};
  const auto partitions = std::vector<PartitionId>(4, PartitionId{0});
  auto bitmap = std::vector<std::byte>(4, std::byte{0});
  stream.scatter(*segment, 0, 4, partitions, 1, heads, store, bitmap.data(), 1, 0);
  heads.finish(store);

  const auto& region = store.value_region(0, 0);
  const auto values = std::span<const std::byte>{region.data(), region.size()};
  const auto null_bitmap = std::vector<std::byte>{std::byte{0b0100}};
  const auto slots = std::vector<uint32_t>{0, 0, 0, 0};
  column.grow_to(1);
  column.fold(slots, values, null_bitmap);
  column.fold(slots, values, null_bitmap);

  EXPECT_EQ(finalize_slots(column, 0, 1, DataType::Long)[0], AllTypeVariant{int64_t{2}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, MinMaxTrackExtremes) {
  const auto table = make_input_table({{"a", DataType::Float, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Min, *table, ColumnID{0}),
                                                             make_aggregate(WindowFunction::Max, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  ASSERT_EQ(columns.size(), 2u);

  const auto slots = std::vector<uint32_t>{0, 0, 0};
  const auto values = pack_values<float>({3.5f, -2.5f, 7.25f});
  for (const auto& column : columns) {
    column->grow_to(1);
    column->fold(slots, values, {});
  }

  EXPECT_EQ(finalize_slots(*columns[0], 0, 1, DataType::Float)[0], AllTypeVariant{-2.5f});
  EXPECT_EQ(finalize_slots(*columns[1], 0, 1, DataType::Float)[0], AllTypeVariant{7.25f});
}

TEST_F(AggregateDYODAccumulatorColumnTest, StringMinMaxCompareLexicographically) {
  const auto table =
      make_input_table({{"a", DataType::String, true}},
                       {{pmr_string{"pear"}}, {pmr_string{"apple"}}, {NullValue{}}, {pmr_string{"zebra"}}});
  const auto segment = table->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Min, *table, ColumnID{0}),
                                                             make_aggregate(WindowFunction::Max, *table, ColumnID{0})};
  const auto schema = AggregateSchema::build(aggregates, *table);
  const auto columns = schema.make_accumulator_columns();

  const auto& stream = schema.value_stream(0);
  const auto value_stream_widths = std::vector<size_t>{stream.element_width()};
  auto store = ScatterStore{PartitionCount{1}, /*key_width=*/4, value_stream_widths, /*value_null_bitmap_width=*/1,
                            /*needs_value_arena=*/true};
  const auto stream_widths = std::vector<size_t>{4, stream.element_width(), 1};
  auto heads = ScatterHeads{PartitionCount{1}, stream_widths.size(), stream_widths, true};
  const auto partitions = std::vector<PartitionId>(4, PartitionId{0});
  auto bitmap = std::vector<std::byte>(4, std::byte{0});
  stream.scatter(*segment, 0, 4, partitions, 1, heads, store, bitmap.data(), 1, 0);
  heads.finish(store);

  const auto& region = store.value_region(0, 0);
  const auto values = std::span<const std::byte>{region.data(), region.size()};
  const auto null_bitmap = std::vector<std::byte>{std::byte{0b0100}};
  const auto slots = std::vector<uint32_t>{0, 0, 0, 0};
  for (const auto& column : columns) {
    column->grow_to(1);
    column->fold(slots, values, null_bitmap);
  }

  EXPECT_EQ(finalize_slots(*columns[0], 0, 1, DataType::String)[0], AllTypeVariant{pmr_string{"apple"}});
  EXPECT_EQ(finalize_slots(*columns[1], 0, 1, DataType::String)[0], AllTypeVariant{pmr_string{"zebra"}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, GrowToPreservesAccumulatedState) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(1);
  column.fold(std::vector<uint32_t>{0}, pack_values<int32_t>({5}), {});
  column.grow_to(3);
  column.fold(std::vector<uint32_t>{0, 2}, pack_values<int32_t>({1, 4}), {});

  const auto results = finalize_slots(column, 0, 3, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{6}});
  EXPECT_TRUE(variant_is_null(results[1]));
  EXPECT_EQ(results[2], AllTypeVariant{int64_t{4}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, ClearResetsForReuse) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(2);
  column.fold(std::vector<uint32_t>{0, 1}, pack_values<int32_t>({8, 9}), {});
  column.clear();

  column.grow_to(1);
  column.fold(std::vector<uint32_t>{0}, pack_values<int32_t>({3}), {});

  const auto results = finalize_slots(column, 0, 1, DataType::Long);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{3}});
}

TEST_F(AggregateDYODAccumulatorColumnTest, FinalizeEmitsTheRequestedSlotRange) {
  const auto table = make_input_table({{"a", DataType::Int, false}}, {});
  const auto aggregates =
      std::vector<std::shared_ptr<WindowFunctionExpression>>{make_aggregate(WindowFunction::Sum, *table, ColumnID{0})};
  const auto columns = AggregateSchema::build(aggregates, *table).make_accumulator_columns();
  auto& column = *columns[0];

  column.grow_to(3);
  column.fold(std::vector<uint32_t>{0, 1, 2}, pack_values<int32_t>({1, 2, 3}), {});

  const auto results = finalize_slots(column, 1, 3, DataType::Long);
  ASSERT_EQ(results.size(), 2u);
  EXPECT_EQ(results[0], AllTypeVariant{int64_t{2}});
  EXPECT_EQ(results[1], AllTypeVariant{int64_t{3}});
}

}  // namespace hyrise
