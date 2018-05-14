#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"
#include "../lib/storage/fixed_string_dictionary_column/fixed_string_vector.hpp"

namespace opossum {

class FixedStringVectorTest : public BaseTest {};

TEST_F(FixedStringVectorTest, SubscriptOperator) {
  auto fixed_string_vector = FixedStringVector(6u);
  fixed_string_vector.push_back("abc");
  fixed_string_vector.push_back("string");

  EXPECT_EQ(fixed_string_vector[0u], "abc");
  EXPECT_EQ(fixed_string_vector[1u], "string");
  if (IS_DEBUG) {
    EXPECT_THROW(fixed_string_vector.push_back("opossum"), std::exception);
  } else {
    fixed_string_vector.push_back("opossum");
    EXPECT_EQ(fixed_string_vector[2u], "opossu");
  }
}

TEST_F(FixedStringVectorTest, AtOperator) {
  auto fixed_string_vector = FixedStringVector(6u);
  fixed_string_vector.push_back("abc");
  fixed_string_vector.push_back("string");

  EXPECT_EQ(fixed_string_vector.at(0u).string(), "abc");
  EXPECT_EQ(fixed_string_vector.at(0u).size(), 3u);
  EXPECT_EQ(fixed_string_vector.at(0u).maximum_length(), 6u);

  EXPECT_EQ(fixed_string_vector.at(1u).string(), "string");

  if (IS_DEBUG) {
    EXPECT_THROW(fixed_string_vector.push_back("opossum"), std::exception);
  } else {
    fixed_string_vector.push_back("opossum");
    EXPECT_EQ(fixed_string_vector.at(2u).string(), "opossu");
  }
}

TEST_F(FixedStringVectorTest, Iterator) {
  auto fixed_string_vector = FixedStringVector(5u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str1");

  for (auto it = fixed_string_vector.begin(); it != fixed_string_vector.end(); ++it) {
    *it = FixedString("abcde");
  }

  EXPECT_EQ(fixed_string_vector[0u], "abcde");
}

TEST_F(FixedStringVectorTest, IteratorConst) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");
  const auto& fixed_string_vector_const = fixed_string_vector;

  auto it_begin = fixed_string_vector_const.begin();
  auto it_end = fixed_string_vector_const.end();
  it_end--;

  EXPECT_EQ(it_begin->string(), "str1");
  EXPECT_EQ(it_end->string(), "str2");

  auto const_it_begin = fixed_string_vector_const.cbegin();
  auto const_it_end = fixed_string_vector_const.cend();
  const_it_end--;

  EXPECT_EQ(const_it_begin->string(), "str1");
  EXPECT_EQ(const_it_end->string(), "str2");
}

TEST_F(FixedStringVectorTest, Allocator) {
  auto fixed_string_vector = FixedStringVector(5u);
  auto fixed_string_vector2 = FixedStringVector(fixed_string_vector, fixed_string_vector.get_allocator());

  EXPECT_EQ(fixed_string_vector.get_allocator(), fixed_string_vector2.get_allocator());
}

TEST_F(FixedStringVectorTest, ReverseIterator) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");
  fixed_string_vector.push_back("str3");

  auto last_value = fixed_string_vector.rbegin();
  auto first_value = fixed_string_vector.rend();
  --first_value;

  EXPECT_EQ(last_value->string(), "str3");
  EXPECT_EQ(first_value->string(), "str1");

  for (auto it = fixed_string_vector.rbegin(); it != fixed_string_vector.rend(); ++it) {
    *it = FixedString("abcd");
  }

  EXPECT_EQ(fixed_string_vector[0u], "abcd");
  EXPECT_EQ(fixed_string_vector[1u], "abcd");
  EXPECT_EQ(fixed_string_vector[2u], "abcd");
}

TEST_F(FixedStringVectorTest, Size) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");
  fixed_string_vector.push_back("str3");

  EXPECT_EQ(fixed_string_vector.size(), 3u);
}

TEST_F(FixedStringVectorTest, Erase) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");
  fixed_string_vector.push_back("str3");

  EXPECT_EQ(fixed_string_vector.size(), 3u);

  auto it = fixed_string_vector.begin();
  ++it;

  fixed_string_vector.erase(it, fixed_string_vector.end());

  EXPECT_EQ(fixed_string_vector.size(), 1u);
  EXPECT_EQ(fixed_string_vector[0u], "str1");
}

TEST_F(FixedStringVectorTest, Shrink) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");
  fixed_string_vector.push_back("str3");
  fixed_string_vector.shrink_to_fit();

  EXPECT_EQ(fixed_string_vector.size(), 3u);

  auto it = fixed_string_vector.begin();
  ++it;
  fixed_string_vector.erase(it, fixed_string_vector.end());

  EXPECT_EQ(fixed_string_vector.size(), 1u);

  fixed_string_vector.shrink_to_fit();

  EXPECT_EQ(fixed_string_vector.capacity(), 4u);
}

TEST_F(FixedStringVectorTest, ConstFixedStringVector) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  const auto& fixed_string_vector2 = FixedStringVector(std::move(fixed_string_vector));
  const auto fixed = fixed_string_vector2.get_string_at(0u);
  EXPECT_EQ(fixed, "str1");
}

TEST_F(FixedStringVectorTest, IteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.begin(), v1.end(), 3};

  EXPECT_EQ(v2[2u], "ghi");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(FixedStringVectorTest, ConstIteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.cbegin(), v1.cend(), 3};
  std::vector<std::string> v3 = {};
  auto v4 = FixedStringVector{v3.cbegin(), v3.cend(), 0};

  EXPECT_EQ(v2[0u], "abc");
  EXPECT_EQ(v2.size(), 3u);
  EXPECT_EQ(v4.size(), 1u);
}

TEST_F(FixedStringVectorTest, DataSize) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.push_back("str1");
  fixed_string_vector.push_back("str2");

  EXPECT_EQ(fixed_string_vector.data_size(), 48u);
}

TEST_F(FixedStringVectorTest, Reserve) {
  auto fixed_string_vector = FixedStringVector(4u);
  fixed_string_vector.reserve(2u);

  EXPECT_EQ(fixed_string_vector.capacity(), 8u);
}

TEST_F(FixedStringVectorTest, Sort) {
  auto fixed_string_vector = FixedStringVector(10u);
  fixed_string_vector.push_back("Larry");
  fixed_string_vector.push_back("Bill");
  fixed_string_vector.push_back("Alexander");
  fixed_string_vector.push_back("Mark");
  fixed_string_vector.push_back("Hasso");

  std::sort(fixed_string_vector.begin(), fixed_string_vector.end());

  EXPECT_EQ(fixed_string_vector[0u], "Alexander");
  EXPECT_EQ(fixed_string_vector[4u], "Mark");
}

TEST_F(FixedStringVectorTest, StringLengthZero) {
  auto fixed_string_vector = FixedStringVector(0u);
  EXPECT_EQ(fixed_string_vector.size(), 1u);
  EXPECT_EQ(fixed_string_vector[0u], "");

  fixed_string_vector.push_back("");
  EXPECT_EQ(fixed_string_vector.size(), 1u);
  EXPECT_EQ(fixed_string_vector[0u], "");
}

TEST_F(FixedStringVectorTest, CompareStdStringToFixedString) {
  auto fixed_string_vector = FixedStringVector(6u);
  fixed_string_vector.push_back("abc");
  fixed_string_vector.push_back("string");

  EXPECT_EQ(fixed_string_vector.at(1u), std::string("string"));
  EXPECT_EQ(fixed_string_vector.at(0u), "abc");
  EXPECT_EQ("abc", fixed_string_vector.at(0u));
}

}  // namespace opossum
