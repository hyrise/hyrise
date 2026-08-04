#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/merge_map.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

using namespace expression_functional;

namespace {

struct MergeInput {
  std::shared_ptr<Table> table;
  std::vector<std::shared_ptr<AbstractSegment>> segment_owners;
  std::vector<const AbstractSegment*> segments;
  std::vector<ColumnID> column_ids;
};

MergeInput make_merge_input(const TableColumnDefinitions& definitions,
                            const std::vector<std::vector<AllTypeVariant>>& rows) {
  auto input = MergeInput{};
  input.table = std::make_shared<Table>(definitions, TableType::Data);
  for (const auto& row : rows) {
    input.table->append(row);
  }
  const auto chunk = input.table->get_chunk(ChunkID{0});
  for (auto column_index = size_t{0}; column_index < definitions.size(); ++column_index) {
    const auto column_id = ColumnID{static_cast<ColumnID::base_type>(column_index)};
    input.column_ids.emplace_back(column_id);
    input.segment_owners.emplace_back(chunk->get_segment(column_id));
    input.segments.emplace_back(input.segment_owners.back().get());
  }
  return input;
}

std::vector<ColumnID> group_by_columns(const MergeInput& input, const size_t group_by_count) {
  return {input.column_ids.begin(), input.column_ids.begin() + group_by_count};
}

template <typename KeySchema>
std::vector<std::byte> pack_key_tile(const KeySchema& schema, const MergeInput& input, const size_t group_by_count,
                                     StringSpillBuffer& spill_buffer) {
  const auto width = schema.packed_width();
  const auto row_count = input.table->row_count();
  auto tile = std::vector<std::byte>(row_count * width);
  const auto segments = std::span<const AbstractSegment* const>{input.segments}.first(group_by_count);
  auto scratch = KeyDecodeScratch{};
  schema.decode(segments, scratch);
  for (auto row = uint32_t{0}; row < row_count; ++row) {
    schema.pack(scratch, ChunkOffset{row}, tile.data() + row * width, spill_buffer);
  }
  return tile;
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

std::vector<AllTypeVariant> column_values(OutputColumns& output, const size_t column_index, const size_t row_count) {
  const auto segments = output.column(column_index).take_segments();
  auto values = std::vector<AllTypeVariant>{};
  for (auto row = uint32_t{0}; row < row_count; ++row) {
    values.emplace_back((*segments[0])[ChunkOffset{row}]);
  }
  return values;
}

}  // namespace

class AggregateDYODMergeMapTest : public BaseTest {};

TEST_F(AggregateDYODMergeMapTest, ResolveAssignsDenseSlotsInFirstSeenOrder) {
  const auto input = make_merge_input({{"a", DataType::Int, false}}, {{5}, {7}, {5}, {9}});
  const auto key_schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);

  EXPECT_EQ(slots, (std::vector<uint32_t>{0, 1, 0, 2}));
  EXPECT_EQ(map.size(), 3u);
}

TEST_F(AggregateDYODMergeMapTest, ResolveReusesSlotsAcrossTiles) {
  const auto input = make_merge_input({{"a", DataType::Int, false}}, {{1}, {2}});
  const auto key_schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  slots.clear();
  map.resolve(tile, slots);

  EXPECT_EQ(slots, (std::vector<uint32_t>{0, 1}));
  EXPECT_EQ(map.size(), 2u);
}

TEST_F(AggregateDYODMergeMapTest, FoldAndFlushEmitGroupedSums) {
  const auto input = make_merge_input({{"a", DataType::Int, false}, {"b", DataType::Int, false}},
                                      {{1, 10}, {2, 20}, {1, 30}, {2, 40}, {1, 50}});
  const auto key_schema = NumericShortKeySchema<4>::build(group_by_columns(input, 1), *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *input.table, ColumnID{1})};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, pack_values<int32_t>({10, 20, 30, 40, 50}), {});

  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"sum", DataType::Long, true}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 2), (std::vector<AllTypeVariant>{1, 2}));
  EXPECT_EQ(column_values(output, 1, 2), (std::vector<AllTypeVariant>{int64_t{90}, int64_t{60}}));
}

TEST_F(AggregateDYODMergeMapTest, FlushWritesEachAggregateToItsOwnColumn) {
  const auto input =
      make_merge_input({{"a", DataType::Int, false}, {"b", DataType::Int, false}}, {{1, 10}, {2, 20}, {1, 30}});
  const auto key_schema = NumericShortKeySchema<4>::build(group_by_columns(input, 1), *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *input.table, ColumnID{1}),
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, pack_values<int32_t>({10, 20, 30}), {});
  map.fold(1, slots, {}, {});

  const auto definitions = TableColumnDefinitions{
      {"a", DataType::Int, false}, {"sum", DataType::Long, true}, {"count", DataType::Long, false}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 2), (std::vector<AllTypeVariant>{1, 2}));
  EXPECT_EQ(column_values(output, 1, 2), (std::vector<AllTypeVariant>{int64_t{40}, int64_t{20}}));
  EXPECT_EQ(column_values(output, 2, 2), (std::vector<AllTypeVariant>{int64_t{2}, int64_t{1}}));
}

TEST_F(AggregateDYODMergeMapTest, CountStarFoldsWithoutValueBytes) {
  const auto input = make_merge_input({{"a", DataType::Int, false}}, {{1}, {1}, {2}});
  const auto key_schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, {}, {});

  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"count", DataType::Long, false}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 2), (std::vector<AllTypeVariant>{1, 2}));
  EXPECT_EQ(column_values(output, 1, 2), (std::vector<AllTypeVariant>{int64_t{2}, int64_t{1}}));
}

TEST_F(AggregateDYODMergeMapTest, IndexGrowthKeepsSlotIdsStable) {
  constexpr auto KEY_COUNT = 300;
  auto rows = std::vector<std::vector<AllTypeVariant>>{};
  for (auto key = int32_t{0}; key < KEY_COUNT; ++key) {
    rows.push_back({key});
  }
  const auto input = make_merge_input({{"a", DataType::Int, false}}, rows);
  const auto key_schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(4);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  EXPECT_EQ(map.size(), static_cast<size_t>(KEY_COUNT));

  auto repeated_slots = std::vector<uint32_t>{};
  map.resolve(tile, repeated_slots);
  EXPECT_EQ(repeated_slots, slots);
  EXPECT_EQ(map.size(), static_cast<size_t>(KEY_COUNT));
}

TEST_F(AggregateDYODMergeMapTest, ClearRetainsCapacityForReuse) {
  const auto first = make_merge_input({{"a", DataType::Int, false}, {"b", DataType::Int, false}}, {{1, 10}, {2, 20}});
  const auto key_schema = NumericShortKeySchema<4>::build(group_by_columns(first, 1), *first.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *first.table, ColumnID{1})};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *first.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto first_tile = pack_key_tile(key_schema, first, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(first_tile, slots);
  map.fold(0, slots, pack_values<int32_t>({10, 20}), {});

  map.clear();
  EXPECT_EQ(map.size(), 0u);

  const auto second = make_merge_input({{"a", DataType::Int, false}, {"b", DataType::Int, false}}, {{3, 7}});
  const auto second_tile = pack_key_tile(key_schema, second, 1, spill);
  slots.clear();
  map.resolve(second_tile, slots);
  EXPECT_EQ(slots, (std::vector<uint32_t>{0}));
  map.fold(0, slots, pack_values<int32_t>({7}), {});

  const auto definitions = TableColumnDefinitions{{"a", DataType::Int, false}, {"sum", DataType::Long, true}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 1), (std::vector<AllTypeVariant>{3}));
  EXPECT_EQ(column_values(output, 1, 1), (std::vector<AllTypeVariant>{int64_t{7}}));
}

TEST_F(AggregateDYODMergeMapTest, ShiftedProbingStillSeparatesDistinctKeys) {
  const auto input = make_merge_input({{"a", DataType::Int, false}}, {{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}});
  const auto key_schema = NumericShortKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<NumericShortKeySchema<4>>{key_schema, 12, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);

  EXPECT_EQ(slots, (std::vector<uint32_t>{0, 1, 2, 3, 4, 5, 6, 7}));
  EXPECT_EQ(map.size(), 8u);
}

TEST_F(AggregateDYODMergeMapTest, SpilledKeysAreReinternedIntoTheMap) {
  const auto long_x = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'x');
  const auto long_y = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'y');
  const auto input = make_merge_input({{"a", DataType::String, false}}, {{long_x}, {long_x}, {long_y}});
  const auto key_schema = StringOnlyKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<StringOnlyKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto scatter_spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 1, scatter_spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, {}, {});
  EXPECT_EQ(slots, (std::vector<uint32_t>{0, 0, 1}));

  scatter_spill.clear();
  const auto garbage = std::vector<std::byte>(3 * long_x.size(), std::byte{'!'});
  scatter_spill.append(garbage.data(), garbage.size());

  auto other_spill = StringSpillBuffer{};
  const auto fresh_tile = pack_key_tile(key_schema, input, 1, other_spill);
  slots.clear();
  map.resolve(fresh_tile, slots);
  EXPECT_EQ(slots, (std::vector<uint32_t>{0, 0, 1}));
  EXPECT_EQ(map.size(), 2u);

  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"count", DataType::Long, false}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 2), (std::vector<AllTypeVariant>{long_x, long_y}));
}

TEST_F(AggregateDYODMergeMapTest, FlushReadsOnlyMapOwnedKeyData) {
  const auto long_string = pmr_string(3 * STRING_BLOB_BYTES_PER_COLUMN, 'x');
  const auto input = make_merge_input({{"a", DataType::String, false}}, {{pmr_string{"short"}}, {long_string}});
  const auto key_schema = StringOnlyKeySchema<4>::build(input.column_ids, *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Count, *input.table, INVALID_COLUMN_ID)};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<StringOnlyKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto scatter_spill = StringSpillBuffer{};
  auto tile = pack_key_tile(key_schema, input, 1, scatter_spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, {}, {});

  std::fill(tile.begin(), tile.end(), std::byte{0xAA});
  scatter_spill.clear();
  const auto garbage = std::vector<std::byte>(2 * long_string.size(), std::byte{'!'});
  scatter_spill.append(garbage.data(), garbage.size());

  const auto definitions = TableColumnDefinitions{{"a", DataType::String, false}, {"count", DataType::Long, false}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 2), (std::vector<AllTypeVariant>{pmr_string{"short"}, long_string}));
  EXPECT_EQ(column_values(output, 1, 2), (std::vector<AllTypeVariant>{int64_t{1}, int64_t{1}}));
}

TEST_F(AggregateDYODMergeMapTest, MixedSchemaKeysGroupEndToEnd) {
  const auto input = make_merge_input(
      {{"a", DataType::Int, false}, {"b", DataType::String, false}, {"c", DataType::Int, false}},
      {{1, pmr_string{"x"}, 10}, {1, pmr_string{"x"}, 5}, {2, pmr_string{"y"}, 7}, {1, pmr_string{"z"}, 1}});
  const auto key_schema = MixedKeySchema<4>::build(group_by_columns(input, 2), *input.table);
  const auto aggregates = std::vector<std::shared_ptr<WindowFunctionExpression>>{
      make_aggregate(WindowFunction::Sum, *input.table, ColumnID{2})};
  const auto aggregate_schema = AggregateSchema::build(aggregates, *input.table);

  auto map = MergeMap<MixedKeySchema<4>>{key_schema, 0, aggregate_schema.make_accumulator_columns()};
  map.reserve(8);

  auto spill = StringSpillBuffer{};
  const auto tile = pack_key_tile(key_schema, input, 2, spill);
  auto slots = std::vector<uint32_t>{};
  map.resolve(tile, slots);
  map.fold(0, slots, pack_values<int32_t>({10, 5, 7, 1}), {});

  const auto definitions = TableColumnDefinitions{
      {"a", DataType::Int, false}, {"b", DataType::String, false}, {"sum", DataType::Long, true}};
  auto output = OutputColumns{definitions, /*seal_threshold=*/1024};
  map.flush_into(output);
  output.seal_all();

  EXPECT_EQ(column_values(output, 0, 3), (std::vector<AllTypeVariant>{1, 2, 1}));
  EXPECT_EQ(column_values(output, 1, 3),
            (std::vector<AllTypeVariant>{pmr_string{"x"}, pmr_string{"y"}, pmr_string{"z"}}));
  EXPECT_EQ(column_values(output, 2, 3), (std::vector<AllTypeVariant>{int64_t{15}, int64_t{7}, int64_t{1}}));
}

}  // namespace hyrise
