#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "table_builder.hpp"
#include "testing_assert.hpp"

namespace opossum {

namespace {
template <typename T>
std::optional<T> get_optional_from_table(const Table& table, uint16_t x, int y) {
  return std::static_pointer_cast<ValueSegment<T>>(table.get_chunk(ChunkID{0})->get_segment(ColumnID{x}))
      ->get_typed_value(y);
}

const auto types = boost::hana::tuple<int32_t, std::optional<float>, pmr_string>();
const auto names = boost::hana::make_tuple("a", "b", "c");
}  // namespace

TEST(TableBuilderTest, CreateColumnsWithCorrectNamesAndTypesAndNullables) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  auto table = table_builder.finish_table();

  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Float, true}, {"c", DataType::String, false}},
      TableType::Data);

  EXPECT_TABLE_EQ_UNORDERED(table, expected_table);

  // TODO(anyone): check "is nullable" in EXPECT_TABLE_EQ_UNORDERED (requires fixing a bunch of tests!),
  // then remove the following check
  EXPECT_EQ(table->columns_are_nullable(), std::vector({false, true, false}));
}

TEST(TableBuilderTest, AppendsRows) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  table_builder.append_row(42, 42.0f, "42");
  table_builder.append_row(42, std::optional<float>{}, "42");
  auto table = table_builder.finish_table();

  // TODO(pascal): use EXPECT_TABLE_EQ_UNORDERED here
  EXPECT_EQ(table->row_count(), 2);
  EXPECT_EQ(table->get_value<int32_t>(ColumnID{0}, 0), 42);
  EXPECT_EQ(table->get_value<pmr_string>(ColumnID{2}, 0), pmr_string{"42"});

  auto optional_value = get_optional_from_table<float>(*table, 1, 0);
  EXPECT_TRUE(optional_value.has_value());
  EXPECT_EQ(optional_value.value(), 42.0f);
  EXPECT_FALSE(get_optional_from_table<float>(*table, 1, 1).has_value());
}

}  // namespace opossum
