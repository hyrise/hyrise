#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"
#include "../lib/storage/value_vector.hpp"

namespace opossum {

class FixedStringVectorTest : public BaseTest {};

TEST_F(FixedStringVectorTest, SubscriptOperatorFixedString) {
  auto valuevector = FixedStringVector(6u);
  valuevector.push_back("abc");
  valuevector.push_back("string");

  EXPECT_EQ(valuevector[0], "abc");
  EXPECT_EQ(valuevector[1], "string");
  if (IS_DEBUG) {
    EXPECT_THROW(valuevector.push_back("opossum"), std::exception);
  } else {
    valuevector.push_back("opossum");
    EXPECT_EQ(valuevector[2], "opossu");
  }
}

TEST_F(FixedStringVectorTest, AtOperatorFixedString) {
  auto valuevector = FixedStringVector(6u);
  valuevector.push_back("abc");
  valuevector.push_back("string");

  EXPECT_EQ(valuevector.at(0).string(), "abc");
  EXPECT_EQ(valuevector.at(0).size(), 3u);
  EXPECT_EQ(valuevector.at(0).maximum_length(), 6u);

  EXPECT_EQ(valuevector.at(1).string(), "string");

  if (IS_DEBUG) {
    EXPECT_THROW(valuevector.push_back("opossum"), std::exception);
  } else {
    valuevector.push_back("opossum");
    EXPECT_EQ(valuevector.at(2).string(), "opossu");
  }
}

TEST_F(FixedStringVectorTest, IteratorFixedString) {
  auto valuevector = FixedStringVector(5u);
  valuevector.push_back("str1");
  valuevector.push_back("str1");

  for (auto it = valuevector.begin(); it != valuevector.end(); ++it) {
    *it = FixedString("abcde");
  }

  EXPECT_EQ(valuevector[0], "abcde");
  }

TEST_F(FixedStringVectorTest, IteratorConstFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  const auto& valuevector_const = valuevector;

  auto it_begin = valuevector_const.begin();
  auto it_end = valuevector_const.end();
  it_end--;

  EXPECT_EQ(it_begin->string(), "str1");
  EXPECT_EQ(it_end->string(), "str2");

  auto const_it_begin = valuevector_const.cbegin();
  auto const_it_end = valuevector_const.cend();
  const_it_end--;

  EXPECT_EQ(const_it_begin->string(), "str1");
  EXPECT_EQ(const_it_end->string(), "str2");
}

TEST_F(FixedStringVectorTest, ReverseIteratorFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  auto last_value = valuevector.rbegin();
  auto first_value = valuevector.rend();
  --first_value;

  EXPECT_EQ(last_value->string(), "str3");
  EXPECT_EQ(first_value->string(), "str1");

  for (auto it = valuevector.rbegin(); it != valuevector.rend(); ++it) {
    *it = FixedString("abcd");
  }

  EXPECT_EQ(valuevector[0], "abcd");
  EXPECT_EQ(valuevector[1], "abcd");
  EXPECT_EQ(valuevector[2], "abcd");
}

TEST_F(FixedStringVectorTest, SizeFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  EXPECT_EQ(valuevector.size(), 3u);
}

TEST_F(FixedStringVectorTest, EraseFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");

  EXPECT_EQ(valuevector.size(), 3u);

  auto it = valuevector.begin();
  ++it;

  valuevector.erase(it, valuevector.end());

  EXPECT_EQ(valuevector.size(), 1u);
  EXPECT_EQ(valuevector[0], "str1");
}

TEST_F(FixedStringVectorTest, ShrinkFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");
  valuevector.push_back("str3");
  valuevector.shrink_to_fit();

  EXPECT_EQ(valuevector.size(), 3u);

  auto it = valuevector.begin();
  ++it;
  valuevector.erase(it, valuevector.end());

  EXPECT_EQ(valuevector.size(), 1u);

  valuevector.shrink_to_fit();

  EXPECT_EQ(valuevector.capacity(), 4u);
}

TEST_F(FixedStringVectorTest, ConstFixedStringVectorFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  const auto& valuevector2 = FixedStringVector(std::move(valuevector));
  const auto fixed = valuevector2[0];
  EXPECT_EQ(fixed, "str1");
}

TEST_F(FixedStringVectorTest, IteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.begin(), v1.end()};

  EXPECT_EQ(v2[2], "ghi");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(FixedStringVectorTest, ConstIteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.cbegin(), v1.cend()};

  EXPECT_EQ(v2[0], "abc");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(FixedStringVectorTest, DataSize) {
  auto valuevector = FixedStringVector(4u);
  valuevector.push_back("str1");
  valuevector.push_back("str2");

  EXPECT_EQ(valuevector.data_size(), 48u);
}

TEST_F(FixedStringVectorTest, ReserveFixedString) {
  auto valuevector = FixedStringVector(4u);
  valuevector.reserve(2u);

  EXPECT_EQ(valuevector.capacity(), 8u);
}

}  // namespace opossum
