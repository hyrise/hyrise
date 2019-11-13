#include <optional>

#include "gtest/gtest.h"

#include "testing_assert.hpp"
#include "utils/load_table.hpp"
#include "utils/sqlite_wrapper.hpp"

namespace opossum {

class SQLiteWrapperTest : public ::testing::Test {
 public:
  void SetUp() override { sqlite_wrapper.emplace(); }

  std::optional<SQLiteWrapper> sqlite_wrapper;
};

TEST_F(SQLiteWrapperTest, CreateTable) {
  const auto expected_table = load_table("resources/test_data/tbl/tpch/sf-0.001/orders.tbl");

  sqlite_wrapper->create_sqlite_table(*expected_table, "t");

  const auto actual_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM t");

  EXPECT_TABLE_EQ(actual_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Lenient,
                  FloatComparisonMode::AbsoluteDifference);
}

TEST_F(SQLiteWrapperTest, CreateTableWithSpecialCharsInText) {
  // Test that loading Tables into SQLite works for Tables containing characters that have special meaning in SQL, e.g.
  // commas and single quotes.

  const auto expected_table = load_table("resources/test_data/tbl/string_string_special_chars.tbl");

  sqlite_wrapper->create_sqlite_table(*expected_table, "t");

  const auto actual_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM t");

  EXPECT_TABLE_EQ(actual_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Lenient,
                  FloatComparisonMode::AbsoluteDifference);
}

TEST_F(SQLiteWrapperTest, CreateTableWithNull) {
  const auto expected_mixed_types_null_100_table =
      load_table("resources/test_data/tbl/sqlite/mixed_types_null_100.tbl");

  sqlite_wrapper->create_sqlite_table(*expected_mixed_types_null_100_table, "mixed_types_null_100");

  const auto actual_mixed_types_null_100_table =
      sqlite_wrapper->main_connection.execute_query("SELECT * FROM mixed_types_null_100");

  EXPECT_TABLE_EQ(actual_mixed_types_null_100_table, expected_mixed_types_null_100_table, OrderSensitivity::Yes,
                  TypeCmpMode::Lenient, FloatComparisonMode::AbsoluteDifference);
}

TEST_F(SQLiteWrapperTest, ReloadTable) {
  const auto expected_table = load_table("resources/test_data/tbl/int_float.tbl");

  sqlite_wrapper->create_sqlite_table(*expected_table, "table_to_copy_from");

  // We do not create the table upfront but still expect it to be identical in the end
  sqlite_wrapper->reset_table_from_copy("resetted_table", "table_to_copy_from");
  const auto resetted_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM resetted_table");

  EXPECT_TABLE_EQ(resetted_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Lenient,
                  FloatComparisonMode::AbsoluteDifference);
}

TEST_F(SQLiteWrapperTest, Update) {
  const auto table = load_table("resources/test_data/tbl/int_float.tbl");
  sqlite_wrapper->create_sqlite_table(*table, "update_table");

  sqlite_wrapper->main_connection.execute_query("UPDATE update_table SET a = a + 1 WHERE b > 457");

  const auto actual_table = sqlite_wrapper->main_connection.execute_query("SELECT * FROM update_table");
  const auto expected_table = load_table("resources/test_data/tbl/int_float_updated.tbl");
  EXPECT_TABLE_EQ(actual_table, expected_table, OrderSensitivity::Yes, TypeCmpMode::Lenient,
                  FloatComparisonMode::AbsoluteDifference);
}

}  // namespace opossum
