#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"

namespace opossum {

class FixedStringTest : public BaseTest {
 public:
  void SetUp() override {}
  std::vector<char> char_vector1 = {'f', 'o', 'o'};
  std::vector<char> char_vector2 = {'b', 'a', 'r', 'b', 'a', 'z'};
  FixedString fixed_string1 = FixedString(&char_vector1[0], 3u);
  FixedString fixed_string2 = FixedString(&char_vector2[0], 6u);
};

TEST_F(FixedStringTest, Constructors) {
  std::vector<char> charvector = {'f', 'o', 'o'};
  std::vector<char> charvector2 = {'b', 'a', 'r', 'b', 'a', 'z'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1, "foo");

  auto str2 = FixedString(str1);
  EXPECT_EQ(str2, "foo");

  if (IS_DEBUG) {
    EXPECT_THROW(str1 = FixedString(&charvector2[0], 6u), std::exception);
  } else {
    str1 = FixedString(&charvector2[0], 6u);
    EXPECT_EQ(str1, "bar");
  }
}

TEST_F(FixedStringTest, StringLength) {
  std::vector<char> char_vector = {'f', 'o', 'o', '\0', '\0'};
  FixedString fixed_string = FixedString(&char_vector[0], 5u);

  EXPECT_EQ(fixed_string1.size(), 3u);
  EXPECT_EQ(fixed_string1.maximum_length(), 3u);
  EXPECT_EQ(fixed_string.size(), 3u);
  EXPECT_EQ(fixed_string.maximum_length(), 5u);
  EXPECT_EQ(fixed_string.string(), "foo");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, CompareStrings) {
  std::vector<char> bar_help = {'b', 'a', 'r', '\0'};
  std::vector<char> bars_help = {'b', 'a', 'r', 's'};
  FixedString bar = FixedString(&bar_help[0], 3u);
  FixedString bar_terminator = FixedString(&bar_help[0], 4u);
  FixedString bars = FixedString(&bars_help[0], 4u);

  EXPECT_TRUE(bar < fixed_string1);
  EXPECT_TRUE(bars < fixed_string1);
  EXPECT_TRUE(bar < bars);
  EXPECT_TRUE(bar_terminator < bars);
  EXPECT_FALSE(bars < bar);
  EXPECT_FALSE(bars < bar_terminator);
  EXPECT_FALSE(bar < bar);

  EXPECT_TRUE(bar == bar);
  EXPECT_TRUE(bar == bar_terminator);
  EXPECT_FALSE(fixed_string2 == bar);
  EXPECT_FALSE(bar == fixed_string2);
}

TEST_F(FixedStringTest, Swap) {
  std::vector<char> char_vector = {'b', 'a', 'r'};
  FixedString fixed_string = FixedString(&char_vector[0], 3u);

  std::swap(fixed_string1, fixed_string);
  EXPECT_EQ(fixed_string1, "bar");
  EXPECT_EQ(fixed_string, "foo");
}

TEST_F(FixedStringTest, Print) {
  std::stringstream sstream;
  sstream << fixed_string1;
  EXPECT_EQ(sstream.str().find("foo"), 0u);
}

TEST_F(FixedStringTest, ImplicitCast) {
  std::string std_string = fixed_string1;
  EXPECT_EQ(std_string, "foo");
}

}  // namespace opossum
