#include "../base_test.hpp"

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

}  // namespace opossum
