#include "gtest/gtest.h"

#include "all_type_variant.hpp"
#include "table_builder.hpp"

namespace opossum {

template <typename T>
std::optional<T> get_optional_from_table(const Table& table, uint16_t x, int y) {
  return std::static_pointer_cast<ValueSegment<T>>(table.get_chunk(ChunkID{0})->get_segment(ColumnID{x}))
      ->get_typed_value(y);
}

const auto types = boost::hana::tuple<int32_t, std::optional<float>, pmr_string>();
const auto names = boost::hana::make_tuple("a", "b", "c");

TEST(TableBuilderTest, CreateColumnsWithCorrectNamesAndTypesAndNullables) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  auto table = table_builder.finish_table();

  EXPECT_EQ(table->column_count(), 3);

  auto expected_names = std::vector<std::string>{"a", "b", "c"};
  for (auto i = size_t{0}; i < expected_names.size(); i++) {
    EXPECT_EQ(table->column_names()[i], expected_names[i]);
  }

  auto expected_types = std::vector{DataType::Int, DataType::Float, DataType::String};
  for (auto i = size_t{0}; i < expected_types.size(); i++) {
    EXPECT_EQ(table->column_data_types()[i], expected_types[i]);
  }

  auto expected_nullables = std::vector{false, true, false};
  for (auto i = size_t{0}; i < expected_nullables.size(); i++) {
    EXPECT_EQ(table->columns_are_nullable()[i], expected_nullables[i]);
  }
}

TEST(TableBuilderTest, AppendsRows) {
  auto table_builder = TableBuilder(4, types, names, UseMvcc::No);
  table_builder.append_row(42, 42.0f, "42");
  table_builder.append_row(42, std::optional<float>{}, "42");
  auto table = table_builder.finish_table();

  EXPECT_EQ(table->row_count(), 2);
  EXPECT_EQ(table->get_value<int32_t>(ColumnID{0}, 0), 42);
  EXPECT_EQ(table->get_value<pmr_string>(ColumnID{2}, 0), pmr_string{"42"});

  auto optional_value = get_optional_from_table<float>(*table, 1, 0);
  EXPECT_TRUE(optional_value.has_value());
  EXPECT_EQ(optional_value.value(), 42.0f);
  EXPECT_FALSE(get_optional_from_table<float>(*table, 1, 1).has_value());
}

}  // namespace opossum
