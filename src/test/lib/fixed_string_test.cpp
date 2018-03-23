#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"

namespace opossum {

class FixedStringTest : public BaseTest {};

TEST_F(FixedStringTest, StringLength) {
  FixedString str1 = FixedString(std::string("astring"));
  FixedString str2 = FixedString(std::string("astring\0\0", 9));

  EXPECT_EQ(str1.size(), 7u);
  EXPECT_EQ(str1.maximum_length(), 7u);
  EXPECT_EQ(str2.size(), 7u);
  EXPECT_EQ(str2.maximum_length(), 9u);
}

TEST_F(FixedStringTest, CharvectorToString) {
  std::vector<char> charvector = {'a', 'b', 'c', 'f'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1.string(), "abc");
}

TEST_F(FixedStringTest, Constructors) {
  std::vector<char> charvector = {'f', 'o', 'o'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1.string(), "foo");

  auto str3 = FixedString("barbaz");
  EXPECT_EQ(str3.string(), "barbaz");

  auto str4 = FixedString(str1);
  EXPECT_EQ(str4.string(), "foo");

  auto str5 = FixedString(std::move(str1));
  EXPECT_EQ(str5.string(), "foo");
}

TEST_F(FixedStringTest, CompareStrings) {
  EXPECT_TRUE(FixedString("abcd") < FixedString("bbcd"));
  EXPECT_TRUE(FixedString("abcd") < FixedString("bcd"));
  EXPECT_TRUE(FixedString("abc") < FixedString("abcd"));
  EXPECT_TRUE(FixedString("abc\0") < FixedString("abcd"));
  EXPECT_FALSE(FixedString("abcdd") < FixedString("abcd"));
  EXPECT_FALSE(FixedString("abcdd") < FixedString("abcd\0"));
  EXPECT_FALSE(FixedString("abcd") < FixedString("abcd"));

  EXPECT_TRUE(FixedString("abcd") == FixedString("abcd"));
  EXPECT_TRUE(FixedString("abcd\0") == FixedString("abcd"));
  EXPECT_FALSE(FixedString("abcd") == FixedString("abc"));
  EXPECT_FALSE(FixedString("abc") == FixedString("abcd"));
  EXPECT_FALSE(FixedString("abc") == FixedString("bbcd"));
}

TEST_F(FixedStringTest, CompareStringsRef) {
  std::vector<char> charvector = {'a', 'b', 'c', 'f'};

  auto str1 = FixedString(&charvector[0], 4);
  auto str2 = FixedString("bbcf");

  EXPECT_TRUE(str1 < str2);
}

TEST_F(FixedStringTest, Swap) {
  auto str1 = FixedString("foo");
  auto str2 = FixedString("bar");

  str1.swap(str2);
  EXPECT_EQ(str1.string(), "bar");
  EXPECT_EQ(str2.string(), "foo");
}

TEST_F(FixedStringTest, Print) {
  auto fs = FixedString("foo");
  std::stringstream sstream;
  sstream << fs;
  EXPECT_EQ(sstream.str().find("foo"), 0u);
}

}  // namespace opossum
