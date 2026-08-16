#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "all_type_variant.hpp"
#include "base_test.hpp"
#include "hyrise.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

class TicketingTest : public BaseTest {};

namespace {

// Inline size of a string column: [length, prefix].
constexpr auto STRING_INLINE_SIZE = sizeof(size_t) + PREFIX_LENGTH;

TableColumnDefinitions int_string_definitions(const bool nullable) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, nullable);
  column_definitions.emplace_back("s", DataType::String, nullable);

  return column_definitions;
}

// Checks that `groups` hands every row of a group the same ticket, every group a different one, and that the tickets
// cover [0, group_count). `expected_group_of_row` names the group each input row belongs to.
void verify_tickets(const GroupKeyData& groups, const std::vector<std::string>& expected_group_of_row) {
  auto group_to_ticket = std::unordered_map<std::string, uint64_t>{};
  auto ticket_to_group = std::unordered_map<uint64_t, std::string>{};

  const auto row_count = expected_group_of_row.size();
  for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
    const auto ticket = groups.tickets[row_index];
    ASSERT_LT(ticket, groups.group_count);

    const auto& group = expected_group_of_row[row_index];
    EXPECT_EQ(group_to_ticket.emplace(group, ticket).first->second, ticket);
    EXPECT_EQ(ticket_to_group.emplace(ticket, group).first->second, group);
  }

  EXPECT_EQ(groups.group_count, group_to_ticket.size());
}

}  // namespace

TEST_F(TicketingTest, RowFormatWithoutNullableColumns) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, false);
  column_definitions.emplace_back("b", DataType::Long, false);

  // Columns are laid out in group-by order, not in table order.
  const auto format = create_row_format(column_definitions, {ColumnID{1}, ColumnID{0}});

  // Without a nullable column there is no null bitmap, so the data starts right at the beginning of the row.
  EXPECT_FALSE(format.stores_nulls);
  EXPECT_EQ(format.data_offset, 0);
  EXPECT_EQ(format.col_offsets, std::vector<uint64_t>({0, sizeof(int64_t)}));
  EXPECT_EQ(format.string_column_count, 0);
  // The key covers the column data only. The row itself is padded to `ROW_ALIGNMENT` (so 16 bytes here).
  EXPECT_EQ(format.key_length, sizeof(int64_t) + sizeof(int32_t));
  EXPECT_EQ(format.row_size, 16);
}

TEST_F(TicketingTest, RowFormatWithNullsAndStrings) {
  const auto format = create_row_format(int_string_definitions(true), {ColumnID{0}, ColumnID{1}});

  // A single nullable column is enough to prepend the null bitmap to every row. The string column occupies
  // [length, prefix] inline plus one pointer in the trailing string-pointer area, which is not part of the key.
  EXPECT_TRUE(format.stores_nulls);
  EXPECT_EQ(format.null_bitmap_offset, 0);
  EXPECT_EQ(format.data_offset, sizeof(uint64_t));
  EXPECT_EQ(format.col_offsets, std::vector<uint64_t>({0, sizeof(int32_t)}));
  EXPECT_EQ(format.key_length, sizeof(uint64_t) + sizeof(int32_t) + STRING_INLINE_SIZE);
  // The key ends after 28 bytes, so the string-pointer area is padded to 32 for alignment.
  EXPECT_EQ(format.string_ptr_offset, 32);
  EXPECT_EQ(format.row_size, format.string_ptr_offset + sizeof(char*));
  EXPECT_EQ(format.string_column_count, 1);
  EXPECT_EQ(RowView({nullptr, format}).string_col_count(), 1);
}

TEST_F(TicketingTest, MaterializeRows) {
  const auto long_value = pmr_string{"a_string_that_exceeds_the_prefix"};
  const auto table = std::make_shared<Table>(int_string_definitions(true), TableType::Data, ChunkOffset{10});
  table->append({int32_t{1}, pmr_string{"short"}});
  table->append({NullValue{}, long_value});
  table->append({int32_t{2}, NullValue{}});

  const auto groupby_column_ids = std::vector<ColumnID>{ColumnID{0}, ColumnID{1}};
  const auto format = create_row_format(table->column_definitions(), groupby_column_ids);

  auto materialized = MaterializedRows{};
  materialized.rows = std::make_unique<uint8_t[]>(3 * format.row_size);
  materialize_rows(format, table->get_chunk(ChunkID{0}), groupby_column_ids, materialized);

  ASSERT_EQ(materialized.row_count, 3);
  // The strings come from a value segment, which owns them, so they are pointed at rather than copied.
  EXPECT_EQ(materialized.string_pointer_needs_copy, std::vector<bool>({false}));

  const auto row_at = [&](const size_t offset) {
    return RowView{materialized.rows.get() + offset * format.row_size, format};
  };

  const auto first = row_at(0);
  EXPECT_EQ(first.null_bitmap(), 0);
  EXPECT_EQ(first.read_value<int32_t>(0), 1);
  EXPECT_EQ(first.string_length(1), 5);
  EXPECT_EQ(std::memcmp(first.string_prefix(1), "short", 5), 0);
  // Short strings fit into the prefix, so they need no heap pointer.
  EXPECT_EQ(first.string_ptr(0), nullptr);

  const auto second = row_at(1);
  EXPECT_EQ(second.null_bitmap(), uint64_t{1} << 0);
  EXPECT_EQ(second.string_length(1), long_value.size());
  ASSERT_NE(second.string_ptr(0), nullptr);
  EXPECT_STREQ(second.string_ptr(0), long_value.c_str());

  const auto third = row_at(2);
  EXPECT_EQ(third.null_bitmap(), uint64_t{1} << 1);
  EXPECT_EQ(third.read_value<int32_t>(0), 2);
}

TEST_F(TicketingTest, ComputeGroupsEmptyTable) {
  const auto table = std::make_shared<Table>(int_string_definitions(false), TableType::Data, ChunkOffset{10});
  ASSERT_EQ(table->chunk_count(), 0);

  EXPECT_EQ(compute_groups({ColumnID{0}}, table)->group_count, 0);
  EXPECT_EQ(compute_groups({ColumnID{0}, ColumnID{1}}, table)->group_count, 0);
}

TEST_F(TicketingTest, ComputeGroupsSingleColumn) {
  auto column_definitions = TableColumnDefinitions{};
  column_definitions.emplace_back("a", DataType::Int, true);
  const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2});
  table->append({int32_t{1}});
  table->append({NullValue{}});
  table->append({int32_t{1}});
  table->append({NullValue{}});
  table->append({int32_t{2}});

  const auto groups = compute_groups({ColumnID{0}}, table);

  // NULL forms a group of its own. The fast path builds no hash table.
  EXPECT_FALSE(groups->has_hash_table);
  verify_tickets(*groups, {"1", "null", "1", "null", "2"});
  EXPECT_EQ(groups->group_count, 3);
}

TEST_F(TicketingTest, ComputeGroupsMultiColumn) {
  const auto table = std::make_shared<Table>(int_string_definitions(true), TableType::Data, ChunkOffset{2});
  table->append({int32_t{1}, pmr_string{"x"}});
  table->append({int32_t{1}, pmr_string{"y"}});
  table->append({int32_t{2}, pmr_string{"x"}});
  table->append({int32_t{1}, pmr_string{"x"}});
  table->append({NullValue{}, pmr_string{"x"}});
  table->append({int32_t{1}, NullValue{}});
  table->append({NullValue{}, pmr_string{"x"}});

  const auto groups = compute_groups({ColumnID{0}, ColumnID{1}}, table);

  // NULLs live in the row's null bitmap here, so they group like any other value combination.
  EXPECT_TRUE(groups->has_hash_table);
  verify_tickets(*groups, {"1|x", "1|y", "2|x", "1|x", "null|x", "1|null", "null|x"});
  EXPECT_EQ(groups->group_count, 5);
}

TEST_F(TicketingTest, ComputeGroupsManyChunksParallel) {
  Hyrise::get().set_scheduler(std::make_shared<NodeQueueScheduler>());

  constexpr auto ROW_COUNT = int32_t{5'000};
  constexpr auto GROUP_COUNT = int32_t{69};

  const auto table = std::make_shared<Table>(int_string_definitions(false), TableType::Data, ChunkOffset{100});
  auto expected = std::vector<std::string>{};
  auto expected_single_column = std::vector<std::string>{};
  expected.reserve(ROW_COUNT);
  expected_single_column.reserve(ROW_COUNT);
  for (auto row = int32_t{0}; row < ROW_COUNT; ++row) {
    const auto group = std::to_string(row % GROUP_COUNT);
    table->append({row % GROUP_COUNT, pmr_string{("group_with_a_long_name_" + group).c_str()}});
    expected.emplace_back(group + "|" + group);
    expected_single_column.emplace_back(group);
  }

  const auto multi_column_groups = compute_groups({ColumnID{0}, ColumnID{1}}, table);
  verify_tickets(*multi_column_groups, expected);
  EXPECT_EQ(multi_column_groups->group_count, GROUP_COUNT);

  const auto single_column_groups = compute_groups({ColumnID{0}}, table);
  verify_tickets(*single_column_groups, expected_single_column);
  EXPECT_EQ(single_column_groups->group_count, GROUP_COUNT);
}

}  // namespace hyrise
