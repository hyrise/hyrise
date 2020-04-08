#include <memory>

#include "base_test.hpp"

#include "storage/fixed_string_dictionary_segment/fixed_string_vector.hpp"

namespace opossum {

class FixedStringVectorTest : public BaseTest {
 protected:
  void SetUp() override {
    std::vector<pmr_string> strings = {"foo", "barbaz", "str3"};
    fixed_string_vector = std::make_shared<FixedStringVector>(FixedStringVector(strings.begin(), strings.end(), 6u));
  }
  std::shared_ptr<FixedStringVector> fixed_string_vector = nullptr;
};

TEST_F(FixedStringVectorTest, IteratorConstructor) {
  std::vector<pmr_string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.begin(), v1.end(), 3};

  EXPECT_EQ(v2[2u], "ghi");
  EXPECT_EQ(v2.size(), 3u);
}

TEST_F(FixedStringVectorTest, SubscriptOperator) {
  EXPECT_EQ((*fixed_string_vector)[0u], "foo");
  EXPECT_EQ((*fixed_string_vector)[1u], "barbaz");
  EXPECT_EQ((*fixed_string_vector)[2u], "str3");
}

TEST_F(FixedStringVectorTest, AtOperator) {
  EXPECT_EQ(fixed_string_vector->at(0u).string(), "foo");
  EXPECT_EQ(fixed_string_vector->at(0u).size(), 3u);
  EXPECT_EQ(fixed_string_vector->at(0u).maximum_length(), 6u);

  EXPECT_EQ(fixed_string_vector->at(1u).string(), "barbaz");
}

TEST_F(FixedStringVectorTest, GetStringAtOperator) {
  EXPECT_EQ(fixed_string_vector->get_string_at(0u), "foo");
  EXPECT_EQ(fixed_string_vector->get_string_at(0u).size(), 3u);
  EXPECT_EQ(fixed_string_vector->get_string_at(1u), "barbaz");
  EXPECT_EQ(fixed_string_vector->get_string_at(1u).size(), 6u);
}

TEST_F(FixedStringVectorTest, Iterator) {
  std::vector<char> helper = {'a', 'b', 'c', 'd', 'e'};
  auto fixed_string = FixedString(&helper[0], 5u);

  for (auto it = fixed_string_vector->begin(); it != fixed_string_vector->end(); ++it) {
    *it = fixed_string;
  }

  EXPECT_EQ((*fixed_string_vector)[0u], "abcde");
}

TEST_F(FixedStringVectorTest, RangeIterator) {
  std::vector<pmr_string> v = {"abc", "def", "ghi"};
  auto fs_vector = FixedStringVector{v.cbegin(), v.cend(), 3};

  auto counter = size_t{0};
  for (auto it = fs_vector.begin(); it != fs_vector.end(); ++counter, ++it) {
    EXPECT_EQ(*it, v[counter]);
  }
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
  std::vector<pmr_string> v = {"abc", "def", "ghi"};
  auto fs_vector = FixedStringVector{v.cbegin(), v.cend(), 3};

  for (const auto& fixed_string : fs_vector) {
    EXPECT_EQ(fixed_string.size(), 3u);
  }
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
  std::vector<pmr_string> v1 = {"abc", "def", "ghi"};
  auto v2 = FixedStringVector{v1.cbegin(), v1.cend(), 3};

  EXPECT_EQ(v2[0u], "abc");
  EXPECT_EQ(v2.size(), 3u);

  std::vector<pmr_string> v3 = {};
  auto v4 = FixedStringVector{v3.cbegin(), v3.cend(), 0};

  EXPECT_EQ(v4.size(), 0u);
}

TEST_F(FixedStringVectorTest, DataSize) {
  EXPECT_EQ(fixed_string_vector->data_size(), sizeof(*fixed_string_vector) + 3u * 6u);
}

TEST_F(FixedStringVectorTest, Reserve) {
  fixed_string_vector->reserve(5u);

  EXPECT_EQ(fixed_string_vector->capacity(), 5u * 6u);
}

TEST_F(FixedStringVectorTest, Sort) {
  std::vector<pmr_string> strings = {"Larry", "Bill", "Alexander", "Mark", "Hasso"};
  auto fixed_string_vector1 = FixedStringVector(strings.begin(), strings.end(), 10u);

  std::sort(fixed_string_vector1.begin(), fixed_string_vector1.end());

  EXPECT_EQ(fixed_string_vector1[0u], "Alexander");
  EXPECT_EQ(fixed_string_vector1[4u], "Mark");
}

// FixedStringsVectors of empty strings have a special handling which needs to be tested.
TEST_F(FixedStringVectorTest, StringLengthZero) {
  std::vector<pmr_string> strings = {"", ""};
  auto fixed_string_vector1 = FixedStringVector(strings.begin(), strings.end(), 0u);
  EXPECT_EQ(fixed_string_vector1.size(), 2u);
  EXPECT_EQ(fixed_string_vector1[0u], "");

  fixed_string_vector1.push_back("");
  EXPECT_EQ(fixed_string_vector1.size(), 3u);
  EXPECT_EQ(fixed_string_vector1[0u], "");
  EXPECT_EQ(fixed_string_vector1[1u], "");
  EXPECT_EQ(fixed_string_vector1[2u], "");

  EXPECT_EQ(fixed_string_vector1.get_string_at(0u), "");
  EXPECT_EQ(fixed_string_vector1.get_string_at(1u), "");
  EXPECT_EQ(fixed_string_vector1.get_string_at(2u), "");
}

TEST_F(FixedStringVectorTest, CompareStdStringToFixedString) {
  EXPECT_EQ(fixed_string_vector->at(0u), "foo");
  EXPECT_EQ("foo", fixed_string_vector->at(0u));
  EXPECT_EQ(fixed_string_vector->at(1u), pmr_string("barbaz"));
}

TEST_F(FixedStringVectorTest, ThrowOnOversizedStrings) {
  std::vector<pmr_string> v = {"abc", "defd", "ghi"};
  EXPECT_THROW((FixedStringVector{v.cbegin(), v.cend(), 3u}), std::logic_error);

  EXPECT_THROW(fixed_string_vector->push_back("opossum"), std::logic_error);
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
