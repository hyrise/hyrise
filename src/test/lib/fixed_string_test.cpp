#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"

namespace opossum {

class FixedStringTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(FixedStringTest, StringLength) {
  FixedString str1 = FixedString(std::string("astring"));

  EXPECT_EQ(str1.size();, 7u);
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
  std::vector<char> a{'a', 'b', 'c', 'f'};
  std::vector<char> b{'b', 'b', 'c', 'f'};

  auto str1 = FixedString(&a[0], 4);
  auto str2 = FixedString(&b[0], 4);
  auto str3 = FixedString(&b[0], 4);

  EXPECT_TRUE(str1 < str2);
  EXPECT_TRUE(str2 == str3);
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
  EXPECT_NE(sstream.str().find("foo"), std::string::npos);
}

}  // namespace opossum
