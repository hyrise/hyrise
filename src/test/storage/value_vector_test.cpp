#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"
#include "../lib/storage/value_vector.hpp"

namespace opossum {

class ValueVectorTest : public BaseTest {
 protected:
  void SetUp() override {}
};

TEST_F(ValueVectorTest, SubscriptOperator) {
  auto valuevector = ValueVector<std::string>();
  valuevector.push_back("abc");

  EXPECT_EQ(valuevector[0], "abc");
}

TEST_F(ValueVectorTest, SubscriptOperatorConst) {
  auto valuevector = ValueVector<std::string>();
  valuevector.push_back("const");
  const auto& const_value_vector = valuevector;

  EXPECT_EQ(const_value_vector[0], "const");
}

TEST_F(ValueVectorTest, PushBack) {
  auto valuevector = ValueVector<int>();
  valuevector.push_back(0);
  auto number = 1;
  valuevector.push_back(number);

  EXPECT_EQ(valuevector[0], 0);
  EXPECT_EQ(valuevector[1], 1);
}

TEST_F(ValueVectorTest, Iterator) {
  auto valuevector = ValueVector<std::string>();
  valuevector.push_back("str1");
  valuevector.push_back("str1");

  for (auto it = valuevector.begin(); it != valuevector.end(); ++it) {
    *it = "abc";
  }

  EXPECT_EQ(valuevector[0], "abc");
}

TEST_F(ValueVectorTest, PushString) {
  auto valuevector = ValueVector<FixedString>(3u);
  valuevector.push_back("abc");
  const auto const_fs = "cde";
  valuevector.push_back(const_fs);

  EXPECT_EQ(valuevector[0].string(), "abc");
}

TEST_F(ValueVectorTest, SubscriptOperatorFixedString) {
  auto valuevector = ValueVector<FixedString>(3u);
  valuevector.push_back("abc");

  EXPECT_EQ(valuevector[0].string(), "abc");
}

TEST_F(ValueVectorTest, AtOperatorFixedString) {
  auto valuevector = ValueVector<FixedString>(3u);
  valuevector.push_back("abc");

  EXPECT_EQ(valuevector.at(0).string(), "abc");
}

TEST_F(ValueVectorTest, IteratorFixedString) {
  auto valuevector = ValueVector<FixedString>(5u);
  valuevector.push_back("str1");
  valuevector.push_back("str1");

  for (auto it = valuevector.begin(); it != valuevector.end(); ++it) {
    *it = FixedString("abcde");
  }

  EXPECT_EQ(valuevector[0].string(), "abcde");
}

TEST_F(ValueVectorTest, ReverseIteratorFixedString) {
  auto valuevector = ValueVector<FixedString>(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  auto last_value = valuevector.rbegin();
  auto first_value = valuevector.rend();
  --first_value;

  EXPECT_EQ(last_value->string(), "str3");
  EXPECT_EQ(first_value->string(), "str1");

  for (auto it = valuevector.rbegin(); it != valuevector.rend(); ++it) {
    *it = FixedString("abcde");
  }

  EXPECT_EQ(valuevector[0].string(), "abcd");
  EXPECT_EQ(valuevector[1].string(), "abcd");
  EXPECT_EQ(valuevector[2].string(), "abcd");
}

TEST_F(ValueVectorTest, SizeFixedString) {
  auto valuevector = ValueVector<FixedString>(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  EXPECT_EQ(valuevector.size(), 3u);
}

TEST_F(ValueVectorTest, EraseFixedString) {
  auto valuevector = ValueVector<FixedString>(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  EXPECT_EQ(valuevector.size(), 3u);

  auto it = valuevector.begin();
  ++it;

  valuevector.erase(it, valuevector.end());

  EXPECT_EQ(valuevector.size(), 1u);
  EXPECT_EQ(valuevector[0].string(), "str1");
}

TEST_F(ValueVectorTest, ShrinkFixedString) {
  auto valuevector = ValueVector<FixedString>(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  auto it = valuevector.begin();
  ++it;

  valuevector.shrink_to_fit();

  EXPECT_EQ(valuevector.size(), 3u);

  valuevector.erase(it, valuevector.end());

  EXPECT_EQ(valuevector.size(), 1u);

  valuevector.shrink_to_fit();

  // TODO(team_btm): test otherwise
  // EXPECT_EQ(valuevector.capacity(), 4u);
}

TEST_F(ValueVectorTest, ConstValueVectorFixedString) {
  auto valuevector = ValueVector<FixedString>(4u);
  valuevector.push_back("str1");
  const auto& valuevector2 = ValueVector<FixedString>(std::move(valuevector));
  const auto fixed = valuevector2[0];
  EXPECT_EQ(fixed.string(), "str1");
}

TEST_F(ValueVectorTest, IteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = ValueVector<std::string>{v1.begin(), v1.end()};

  EXPECT_EQ(v2[2], "ghi");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(ValueVectorTest, ConstIteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = ValueVector<std::string>{v1.cbegin(), v1.cend()};

  EXPECT_EQ(v2[0], "abc");
  EXPECT_EQ(v2.size(), 3u);
}

}  // namespace opossum
