#include "base_test.hpp"

#include "storage/table.hpp"
#include "utils/string_utils.hpp"

namespace opossum {

class StringUtilsTest : public BaseTest {};

TEST_F(StringUtilsTest, trim_and_split) {
  const std::string test_command = "print opossonauten_table";

  auto arguments = trim_and_split(test_command);
  EXPECT_EQ(arguments.size(), 2);
  EXPECT_EQ(arguments[0], "print");
  EXPECT_EQ(arguments[1], "opossonauten_table");
}

TEST_F(StringUtilsTest, trim_and_split_whitespace_padding) {
  const std::string test_command = "   print opossonauten_table  ";

  auto arguments = trim_and_split(test_command);
  EXPECT_EQ(arguments.size(), 2);
  EXPECT_EQ(arguments[0], "print");
  EXPECT_EQ(arguments[1], "opossonauten_table");
}

TEST_F(StringUtilsTest, trim_and_split_double_spaces) {
  const std::string test_command = "print  opossonauten_table";

  auto arguments = trim_and_split(test_command);
  EXPECT_EQ(arguments.size(), 2);
  EXPECT_EQ(arguments[0], "print");
  EXPECT_EQ(arguments[1], "opossonauten_table");
}

TEST_F(StringUtilsTest, trim_source_file_path) {
  EXPECT_EQ(trim_source_file_path("/home/user/checkout/src/file.cpp"), "src/file.cpp");
  EXPECT_EQ(trim_source_file_path("hello/file.cpp"), "hello/file.cpp");
}

TEST_F(StringUtilsTest, table_key_constraints_to_stream) {
  const auto table = Table::create_dummy_table(
      {{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Int, false}});
  table->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
  table->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});
  const auto separator = std::string{" | "};
  auto stream = std::ostringstream{};

  table_key_constraints_to_stream(stream, table, separator);

  EXPECT_EQ(stream.str(), "PRIMARY_KEY(c) | UNIQUE(a, b)");
}

}  // namespace opossum
