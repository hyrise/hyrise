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
  auto str1 = FixedString(std::string("astring"));

  EXPECT_EQ(str1.size(), 7u);
}

TEST_F(FixedStringTest, CharvectorToString) {
  std::vector<char> charvector = {'a', 'b', 'c', 'f'};

  auto str1 = FixedString(&charvector[0], 3);
  EXPECT_EQ(str1.string(), "abc");
}

TEST_F(FixedStringTest, Constructors) {
  std::vector<char> charvector = {'f', 'o', 'o'};

  auto str1 = FixedString(&charvector[0], 3);
  auto str2 = FixedString("bar");
  auto str3 = FixedString(str1);

  EXPECT_EQ(str3.string(), "foo");
  EXPECT_EQ(str2.size(), 3u);
}

TEST_F(FixedStringTest, CompareStrings) {
  std::vector<char> a {'a', 'b', 'c', 'f'};
  std::vector<char> b {'b', 'b', 'c', 'f'};

  auto str1 = FixedString(&a[0], 4);
  auto str2 = FixedString(&b[0], 4);

  EXPECT_TRUE(str1 < str2);
}

TEST_F(FixedStringTest, CompareStringsRef) {
  std::vector<char> charvector = {'a', 'b', 'c', 'f'};

  auto str1 = FixedString(&charvector[0], 4);
  auto str2 = FixedString("bbcf");

  EXPECT_TRUE(str1 < str2);
}

}  // namespace opossum
