#include "../base_test.hpp"

#include "all_type_variant.hpp"
#include "table_builder.hpp"

namespace opossum {

namespace {
const auto table_builder_test_types = boost::hana::tuple<int32_t, std::optional<float>, pmr_string>();
const auto table_builder_test_names = boost::hana::make_tuple("a", "b", "c");
}  // namespace

class TableBuilderTest : public BaseTest {};

TEST_F(TableBuilderTest, CreateColumnsWithCorrectNamesAndTypesAndNullables) {
  auto table_builder = TableBuilder(4, table_builder_test_types, table_builder_test_names);
  const auto table = table_builder.finish_table();

  const auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Float, true}, {"c", DataType::String, false}},
      TableType::Data);

  EXPECT_TABLE_EQ_UNORDERED(table, expected_table);
  EXPECT_EQ(table->columns_are_nullable(), std::vector({false, true, false}));
}

TEST_F(TableBuilderTest, AppendsRows) {
  auto table_builder = TableBuilder(4, table_builder_test_types, table_builder_test_names);
  table_builder.append_row(42, 42.0f, "42");
  table_builder.append_row(43, std::optional<float>{}, "43");
  const auto table = table_builder.finish_table();

  auto expected_table = std::make_shared<Table>(
      TableColumnDefinitions{{"a", DataType::Int, false}, {"b", DataType::Float, true}, {"c", DataType::String, false}},
      TableType::Data);
  expected_table->append({42, 42.0f, "42"});
  expected_table->append({43, NULL_VALUE, "43"});

  EXPECT_TABLE_EQ_UNORDERED(table, expected_table);
}

}  // namespace opossum
