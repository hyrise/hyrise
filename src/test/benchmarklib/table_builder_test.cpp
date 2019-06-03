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
  const auto table = table_builder.finish_table();

  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int}, {"b", DataType::Float}, {"c", DataType::String}}, TableType::Data);

  // TODO(anyone): as soon as 'nullable' is checked in check_table_equal this will fail - please remove the
  //  EXPECT_EQ and replace the TableColumnDefinitions above with:
  //  TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Float, true}, {"c", DataType::String, false}},
  EXPECT_TABLE_EQ_UNORDERED(table, expected_table);
  EXPECT_EQ(table->columns_are_nullable(), std::vector({false, true, false}));
}

TEST(TableBuilderTest, AppendsRows) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  table_builder.append_row(42, 42.0f, "42");
  table_builder.append_row(42, std::optional<float>{}, "42");
  const auto table = table_builder.finish_table();

  auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int}, {"b", DataType::Float, true}, {"c", DataType::String}},
      TableType::Data);
  expected_table->append({42, 42.0f, "42"});
  expected_table->append({42, NULL_VALUE, "42"});

  EXPECT_TABLE_EQ_UNORDERED(table, expected_table);
}

}  // namespace opossum
