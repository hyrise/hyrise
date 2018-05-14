#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/fixed_string.hpp"
#include "../lib/storage/fixed_string_dictionary_column/fixed_string_vector.hpp"

namespace opossum {

class FixedStringVectorTest : public BaseTest {};

TEST_F(FixedStringVectorTest, SubscriptOperator) {
  auto fixedstring_vector = FixedStringVector(6u);
  fixedstring_vector.push_back("abc");
  fixedstring_vector.push_back("string");

  EXPECT_EQ(fixedstring_vector[0u], "abc");
  EXPECT_EQ(fixedstring_vector[1u], "string");
  if (IS_DEBUG) {
    EXPECT_THROW(fixedstring_vector.push_back("opossum"), std::exception);
  } else {
    fixedstring_vector.push_back("opossum");
    EXPECT_EQ(fixedstring_vector[2u], "opossu");
  }
}

TEST_F(FixedStringVectorTest, AtOperator) {
  auto fixedstring_vector = FixedStringVector(6u);
  fixedstring_vector.push_back("abc");
  fixedstring_vector.push_back("string");

  EXPECT_EQ(fixedstring_vector.at(0u).string(), "abc");
  EXPECT_EQ(fixedstring_vector.at(0u).size(), 3u);
  EXPECT_EQ(fixedstring_vector.at(0u).maximum_length(), 6u);

  EXPECT_EQ(fixedstring_vector.at(1u).string(), "string");

  if (IS_DEBUG) {
    EXPECT_THROW(fixedstring_vector.push_back("opossum"), std::exception);
  } else {
    fixedstring_vector.push_back("opossum");
    EXPECT_EQ(fixedstring_vector.at(2u).string(), "opossu");
  }
}

TEST_F(FixedStringVectorTest, Iterator) {
  auto fixedstring_vector = FixedStringVector(5u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str1");

  for (auto it = fixedstring_vector.begin(); it != fixedstring_vector.end(); ++it) {
    *it = FixedString("abcde");
  }

  EXPECT_EQ(fixedstring_vector[0u], "abcde");
}

TEST_F(FixedStringVectorTest, IteratorConst) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");
  const auto& fixedstring_vector_const = fixedstring_vector;

  auto it_begin = fixedstring_vector_const.begin();
  auto it_end = fixedstring_vector_const.end();
  it_end--;

  EXPECT_EQ(it_begin->string(), "str1");
  EXPECT_EQ(it_end->string(), "str2");

  auto const_it_begin = fixedstring_vector_const.cbegin();
  auto const_it_end = fixedstring_vector_const.cend();
  const_it_end--;

  EXPECT_EQ(const_it_begin->string(), "str1");
  EXPECT_EQ(const_it_end->string(), "str2");
}

TEST_F(FixedStringVectorTest, Allocator) {
  auto fixedstring_vector = FixedStringVector(5u);
  auto fixedstring_vector2 = FixedStringVector(fixedstring_vector, fixedstring_vector.get_allocator());

  EXPECT_EQ(fixedstring_vector.get_allocator(), fixedstring_vector2.get_allocator());
}

TEST_F(FixedStringVectorTest, ReverseIterator) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");
  fixedstring_vector.push_back("str3");

  auto last_value = fixedstring_vector.rbegin();
  auto first_value = fixedstring_vector.rend();
  --first_value;

  EXPECT_EQ(last_value->string(), "str3");
  EXPECT_EQ(first_value->string(), "str1");

  for (auto it = fixedstring_vector.rbegin(); it != fixedstring_vector.rend(); ++it) {
    *it = FixedString("abcd");
  }

  EXPECT_EQ(fixedstring_vector[0u], "abcd");
  EXPECT_EQ(fixedstring_vector[1u], "abcd");
  EXPECT_EQ(fixedstring_vector[2u], "abcd");
}

TEST_F(FixedStringVectorTest, Size) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");
  fixedstring_vector.push_back("str3");

  EXPECT_EQ(fixedstring_vector.size(), 3u);
}

TEST_F(FixedStringVectorTest, Erase) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");
  fixedstring_vector.push_back("str3");

  EXPECT_EQ(fixedstring_vector.size(), 3u);

  auto it = fixedstring_vector.begin();
  ++it;

  fixedstring_vector.erase(it, fixedstring_vector.end());

  EXPECT_EQ(fixedstring_vector.size(), 1u);
  EXPECT_EQ(fixedstring_vector[0u], "str1");
}

TEST_F(FixedStringVectorTest, Shrink) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");
  fixedstring_vector.push_back("str3");
  fixedstring_vector.shrink_to_fit();

  EXPECT_EQ(fixedstring_vector.size(), 3u);

  auto it = fixedstring_vector.begin();
  ++it;
  fixedstring_vector.erase(it, fixedstring_vector.end());

  EXPECT_EQ(fixedstring_vector.size(), 1u);

  fixedstring_vector.shrink_to_fit();

  EXPECT_EQ(fixedstring_vector.capacity(), 4u);
}

TEST_F(FixedStringVectorTest, ConstFixedStringVector) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  const auto& fixedstring_vector2 = FixedStringVector(std::move(fixedstring_vector));
  const auto fixed = fixedstring_vector2.get_string_at(0u);
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
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.push_back("str1");
  fixedstring_vector.push_back("str2");

  EXPECT_EQ(fixedstring_vector.data_size(), 48u);
}

TEST_F(FixedStringVectorTest, Reserve) {
  auto fixedstring_vector = FixedStringVector(4u);
  fixedstring_vector.reserve(2u);

  EXPECT_EQ(fixedstring_vector.capacity(), 8u);
}

TEST_F(FixedStringVectorTest, Sort) {
  auto fixedstring_vector = FixedStringVector(10u);
  fixedstring_vector.push_back("Larry");
  fixedstring_vector.push_back("Bill");
  fixedstring_vector.push_back("Alexander");
  fixedstring_vector.push_back("Mark");
  fixedstring_vector.push_back("Hasso");

  std::sort(fixedstring_vector.begin(), fixedstring_vector.end());

  EXPECT_EQ(fixedstring_vector[0u], "Alexander");
  EXPECT_EQ(fixedstring_vector[4u], "Mark");
}

TEST_F(FixedStringVectorTest, StringLengthZero) {
  auto fixedstring_vector = FixedStringVector(0u);
  EXPECT_EQ(fixedstring_vector.size(), 1u);
  EXPECT_EQ(fixedstring_vector[0u], "");

  fixedstring_vector.push_back("");
  EXPECT_EQ(fixedstring_vector.size(), 1u);
  EXPECT_EQ(fixedstring_vector[0u], "");
}

TEST_F(FixedStringVectorTest, CompareStdStringToFixedString) {
  auto fixedstring_vector = FixedStringVector(6u);
  fixedstring_vector.push_back("abc");
  fixedstring_vector.push_back("string");

  EXPECT_EQ(fixedstring_vector.at(1u), std::string("string"));
  EXPECT_EQ(fixedstring_vector.at(0u), "abc");
  EXPECT_EQ("abc", fixedstring_vector.at(0u));
}

}  // namespace opossum
