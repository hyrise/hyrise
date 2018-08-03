#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/fixed_string_dictionary_column/fixed_string_vector.hpp"

namespace opossum {

class FixedStringVectorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::vector<std::string> strings = {"foo", "barbaz", "str3"};
    fixed_string_vector =
        std::make_shared<FixedStringVector>(FixedStringVector(strings.begin(), strings.end(), 6u, strings.size()));
  }
  std::shared_ptr<FixedStringVector> fixed_string_vector = nullptr;
};

TEST_F(FixedStringVectorTest, IteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.begin(), v1.end(), 3, 3u};

  EXPECT_EQ(v2[2u], "ghi");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(FixedStringVectorTest, SubscriptOperator) {
  EXPECT_EQ((*fixed_string_vector)[0u], "foo");
  EXPECT_EQ((*fixed_string_vector)[1u], "barbaz");
  EXPECT_EQ((*fixed_string_vector)[2u], "str3");
  if (IS_DEBUG) {
    EXPECT_THROW(fixed_string_vector->push_back("opossum"), std::exception);
  } else {
    fixed_string_vector->push_back("opossum");
    EXPECT_EQ((*fixed_string_vector)[3u], "opossu");
  }
}

TEST_F(FixedStringVectorTest, AtOperator) {
  EXPECT_EQ(fixed_string_vector->at(0u).string(), "foo");
  EXPECT_EQ(fixed_string_vector->at(0u).size(), 3u);
  EXPECT_EQ(fixed_string_vector->at(0u).maximum_length(), 6u);

  EXPECT_EQ(fixed_string_vector->at(1u).string(), "barbaz");

  if (IS_DEBUG) {
    EXPECT_THROW(fixed_string_vector->push_back("opossum"), std::exception);
  } else {
    fixed_string_vector->push_back("opossum");
    EXPECT_EQ(fixed_string_vector->at(3u).string(), "opossu");
  }
}

TEST_F(FixedStringVectorTest, Iterator) {
  std::vector<char> helper = {'a', 'b', 'c', 'd', 'e'};
  auto fixed_string = FixedString(&helper[0], 5u);

  for (auto it = fixed_string_vector->begin(); it != fixed_string_vector->end(); ++it) {
    *it = fixed_string;
  }

  EXPECT_EQ((*fixed_string_vector)[0u], "abcde");
}

TEST_F(FixedStringVectorTest, IteratorConst) {
  const auto& fixed_string_vector_const = *fixed_string_vector;

  auto it_begin = fixed_string_vector_const.begin();
  auto it_end = fixed_string_vector_const.end();
  it_end--;

  EXPECT_EQ(*it_begin, "foo");
  EXPECT_EQ(*it_end, "str3");

  auto const_it_begin = fixed_string_vector_const.cbegin();
  auto const_it_end = fixed_string_vector_const.cend();
  const_it_end--;

  EXPECT_EQ(*const_it_begin, "foo");
  EXPECT_EQ(*const_it_end, "str3");
}

TEST_F(FixedStringVectorTest, ReverseIterator) {
  std::vector<char> helper = {'a', 'b', 'c', 'd'};
  auto fixed_string = FixedString(&helper[0], 4u);

  auto last_value = fixed_string_vector->rbegin();
  auto first_value = fixed_string_vector->rend();
  --first_value;

  EXPECT_EQ(*last_value, "str3");
  EXPECT_EQ(*first_value, "foo");

  for (auto it = fixed_string_vector->rbegin(); it != fixed_string_vector->rend(); ++it) {
    *it = fixed_string;
  }

  EXPECT_EQ((*fixed_string_vector)[0u], "abcd");
  EXPECT_EQ((*fixed_string_vector)[1u], "abcd");
  EXPECT_EQ((*fixed_string_vector)[2u], "abcd");
}

TEST_F(FixedStringVectorTest, Size) { EXPECT_EQ(fixed_string_vector->size(), 3u); }

TEST_F(FixedStringVectorTest, Erase) {
  EXPECT_EQ(fixed_string_vector->size(), 3u);
  auto it = fixed_string_vector->begin();

  fixed_string_vector->erase(++it, fixed_string_vector->end());

  EXPECT_EQ(fixed_string_vector->size(), 1u);
  EXPECT_EQ((*fixed_string_vector)[0u], "foo");
}

TEST_F(FixedStringVectorTest, Shrink) {
  fixed_string_vector->shrink_to_fit();

  EXPECT_EQ(fixed_string_vector->size(), 3u);

  auto it = fixed_string_vector->begin();
  fixed_string_vector->erase(++it, fixed_string_vector->end());

  EXPECT_EQ(fixed_string_vector->size(), 1u);
}

TEST_F(FixedStringVectorTest, ConstFixedStringVector) {
  const auto& fixed_string_vector2 = FixedStringVector(std::move(*fixed_string_vector));
  const auto fixed = fixed_string_vector2.get_string_at(0u);
  EXPECT_EQ(fixed, "foo");
}

TEST_F(FixedStringVectorTest, ConstIteratorConstructor) {
  std::vector<std::string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.cbegin(), v1.cend(), 3, 3};
  std::vector<std::string> v3 = {};
  auto v4 = FixedStringVector{v3.cbegin(), v3.cend(), 0, 0};

  EXPECT_EQ(v2[0u], "abc");
  EXPECT_EQ(v2.size(), 3u);
  EXPECT_EQ(v4.size(), 1u);
}

TEST_F(FixedStringVectorTest, DataSize) { EXPECT_EQ(fixed_string_vector->data_size(), 58u); }

TEST_F(FixedStringVectorTest, Reserve) {
  fixed_string_vector->reserve(5u);

  EXPECT_EQ(fixed_string_vector->capacity(), 5u * 6u);
}

TEST_F(FixedStringVectorTest, Sort) {
  std::vector<std::string> strings = {"Larry", "Bill", "Alexander", "Mark", "Hasso"};
  auto fixed_string_vector1 = FixedStringVector(strings.begin(), strings.end(), 10u, 5u);

  std::sort(fixed_string_vector1.begin(), fixed_string_vector1.end());

  EXPECT_EQ(fixed_string_vector1[0u], "Alexander");
  EXPECT_EQ(fixed_string_vector1[4u], "Mark");
}

TEST_F(FixedStringVectorTest, StringLengthZero) {
  std::vector<std::string> strings = {"", ""};
  auto fixed_string_vector1 = FixedStringVector(strings.begin(), strings.end(), 0u, 0u);
  EXPECT_EQ(fixed_string_vector1.size(), 1u);
  EXPECT_EQ(fixed_string_vector1[0u], "");

  fixed_string_vector1.push_back("");
  EXPECT_EQ(fixed_string_vector1.size(), 1u);
  EXPECT_EQ(fixed_string_vector1[0u], "");
}

TEST_F(FixedStringVectorTest, CompareStdStringToFixedString) {
  EXPECT_EQ(fixed_string_vector->at(0u), "foo");
  EXPECT_EQ("foo", fixed_string_vector->at(0u));
  EXPECT_EQ(fixed_string_vector->at(1u), std::string("barbaz"));
}

TEST_F(FixedStringVectorTest, MemoryLayout) {
  EXPECT_EQ(*fixed_string_vector->data(), 'f');
  EXPECT_EQ(*(fixed_string_vector->data() + 1), 'o');
  EXPECT_EQ(*(fixed_string_vector->data() + 2), 'o');
  EXPECT_EQ(*(fixed_string_vector->data() + 3), '\0');
  EXPECT_EQ(*(fixed_string_vector->data() + 4), '\0');
  EXPECT_EQ(*(fixed_string_vector->data() + 5), '\0');
  EXPECT_EQ(*(fixed_string_vector->data() + 6), 'b');
  EXPECT_EQ(*(fixed_string_vector->data() + 7), 'a');
  EXPECT_EQ(*(fixed_string_vector->data() + 8), 'r');
  EXPECT_EQ(*(fixed_string_vector->data() + 9), 'b');
  EXPECT_EQ(*(fixed_string_vector->data() + 10), 'a');
  EXPECT_EQ(*(fixed_string_vector->data() + 11), 'z');
  EXPECT_EQ(*(fixed_string_vector->data() + 12), 's');
  EXPECT_EQ(*(fixed_string_vector->data() + 13), 't');
  EXPECT_EQ(*(fixed_string_vector->data() + 14), 'r');
  EXPECT_EQ(*(fixed_string_vector->data() + 15), '3');
  EXPECT_EQ(*(fixed_string_vector->data() + 16), '\0');
  EXPECT_EQ(*(fixed_string_vector->data() + 17), '\0');
}

}  // namespace opossum
