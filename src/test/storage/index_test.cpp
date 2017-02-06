#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/common.hpp"
#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/dictionary_column.hpp"
#include "../lib/storage/group_key_index.hpp"
#include "../lib/types.hpp"

namespace opossum {

class IndexTest : public BaseTest {
 protected:
  void SetUp() override {
    dict_col_int = BaseTest::create_dict_column_by_type<int>("int", {3, 4, 0, 4, 2, 7, 8, 1, 4, 9});
    index_int = make_shared_by_column_type<BaseIndex, GroupKeyIndex>("int", dict_col_int);

    dict_col_str =
        BaseTest::create_dict_column_by_type<std::string>("string", {"hello", "world", "test", "foo", "bar", "foo"});
    index_str = make_shared_by_column_type<BaseIndex, GroupKeyIndex>("string", dict_col_str);
  }

  template <class Iterator>
  static std::vector<AllTypeVariant> result_as_vector(std::shared_ptr<BaseColumn> col, Iterator begin, Iterator end) {
    std::vector<AllTypeVariant> result{};
    for (auto iter(std::move(begin)); iter != end; ++iter) {
      result.emplace_back((*col)[*iter]);
    }
    return result;
  }

  std::shared_ptr<BaseIndex> index_int = nullptr;
  std::shared_ptr<BaseColumn> dict_col_int = nullptr;
  std::shared_ptr<BaseIndex> index_str = nullptr;
  std::shared_ptr<BaseColumn> dict_col_str = nullptr;
};

TEST_F(IndexTest, FullRange) {
  auto begin_int = index_int->begin();
  auto end_int = index_int->end();
  auto result_values_int = result_as_vector(dict_col_int, begin_int, end_int);
  auto expected_values_int = std::vector<AllTypeVariant>{0, 1, 2, 3, 4, 4, 4, 7, 8, 9};
  EXPECT_EQ(expected_values_int, result_values_int);

  auto begin_str = index_str->begin();
  auto end_str = index_str->end();
  auto result_values_str = result_as_vector(dict_col_str, begin_str, end_str);
  auto expected_values_str = std::vector<AllTypeVariant>{"bar", "foo", "foo", "hello", "test", "world"};
  EXPECT_EQ(expected_values_str, result_values_str);
}

TEST_F(IndexTest, PointQueryWithSingleReturnValue) {
  auto begin = index_int->lower_bound({3});
  auto end = index_int->upper_bound({3});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{0};

  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, PointQueryWithNoReturnValue) {
  auto begin = index_int->lower_bound({5});
  auto end = index_int->upper_bound({5});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, PointQueryWithMultipleReturnValues) {
  auto begin = index_int->lower_bound({4});
  auto end = index_int->upper_bound({4});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{1, 3, 8};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQuery) {
  auto begin = index_int->lower_bound({1});
  auto end = index_int->upper_bound({3});

  auto result_positions = std::set<std::size_t>(begin, end);
  auto result_values = result_as_vector(dict_col_int, begin, end);

  auto expected_positions = std::set<std::size_t>{0, 4, 7};
  auto expected_values = std::vector<AllTypeVariant>{1, 2, 3};

  EXPECT_EQ(expected_positions, result_positions);
  EXPECT_EQ(expected_values, result_values);
}

TEST_F(IndexTest, RangeQueryBelow) {
  auto begin = index_int->lower_bound({-3});
  auto end = index_int->upper_bound({-1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQueryPartiallyBelow) {
  auto begin = index_int->lower_bound({-3});
  auto end = index_int->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQueryAbove) {
  auto begin = index_int->lower_bound({10});
  auto end = index_int->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQueryPartiallyAbove) {
  auto begin = index_int->lower_bound({8});
  auto end = index_int->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQueryOpenEnd) {
  auto begin = index_int->lower_bound({8});
  auto end = index_int->end();

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, RangeQueryOpenBegin) {
  auto begin = index_int->begin();
  auto end = index_int->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TEST_F(IndexTest, IsIndexForTest) {
  EXPECT_TRUE(index_int->is_index_for({dict_col_int}));
  EXPECT_TRUE(index_str->is_index_for({dict_col_str}));

  EXPECT_FALSE(index_int->is_index_for({dict_col_str}));
  EXPECT_FALSE(index_str->is_index_for({dict_col_int}));
  EXPECT_FALSE(index_str->is_index_for({dict_col_str, dict_col_int}));
  EXPECT_FALSE(index_str->is_index_for({}));
}

}  // namespace opossum
