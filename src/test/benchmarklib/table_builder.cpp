#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "table_builder.hpp"

namespace opossum {

const auto types = boost::hana::tuple<int32_t, std::optional<float>, pmr_string>();
const auto names = boost::hana::make_tuple("a", "b", "c");

TEST(TableBuilderTest, CreateColumnsWithCorrectNamesAndTypesAndNullables) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  auto table = table_builder.finish_table();
//
//  EXPECT_EQ(table->column_count(), 3);
//
//  auto expected_names = std::vector<pmr_string>{"a", "b", "c"};
//  EXPECT_EQ(table->column_names(), expected_names);
//
//  auto expected_types = std::vector{DataType::Int, DataType::Float, DataType::String};
//  EXPECT_EQ(table->column_data_types(), expected_types);
//
//  auto expected_nullables = std::vector{false, true, true};
//  EXPECT_EQ(table->columns_are_nullable(), expected_nullables);
}

TEST(TableBuilderTest, AppendsRows) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  table_builder.append_row(42, 42.0f, "42");
  table_builder.append_row(42, std::optional<float>{}, "42");
  auto table = table_builder.finish_table();
//
//  EXPECT_EQ(table->row_count(), 2);
//  EXPECT_TRUE(table->get_value(1, 0).has_value());
//  EXPECT_FALSE(table->get_value(1, 1).has_value());
}

}  // namespace opossum
