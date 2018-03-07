#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../lib/storage/base_column.hpp"
#include "../lib/storage/chunk.hpp"
#include "../lib/storage/index/group_key/composite_group_key_index.hpp"
#include "../lib/types.hpp"

namespace opossum {

template <typename DerivedIndex>
class MultiColumnIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    dict_col_int = BaseTest::create_dict_column_by_type<int>(DataType::Int, {3, 4, 0, 4, 2, 7, 8, 4, 1, 9});
    dict_col_str = BaseTest::create_dict_column_by_type<std::string>(
        DataType::String, {"foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz", "foo"});

    index_int_str =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dict_col_int, dict_col_str});
    index_str_int =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseColumn>>{dict_col_str, dict_col_int});
  }

  template <class Iterator>
  static std::vector<std::vector<AllTypeVariant>> result_as_vector(const std::vector<std::shared_ptr<BaseColumn>> cols,
                                                                   Iterator begin, Iterator end) {
    std::vector<std::vector<AllTypeVariant>> result{};
    for (auto iter(std::move(begin)); iter != end; ++iter) {
      auto row = std::vector<AllTypeVariant>{};
      for (auto column : cols) {
        row.emplace_back((*column)[*iter]);
      }
      result.emplace_back(row);
    }
    return result;
  }

  std::shared_ptr<BaseIndex> index_int_str = nullptr;
  std::shared_ptr<BaseIndex> index_str_int = nullptr;
  std::shared_ptr<BaseColumn> dict_col_int = nullptr;
  std::shared_ptr<BaseColumn> dict_col_str = nullptr;
};

// List of indices to test
typedef ::testing::Types<CompositeGroupKeyIndex> DerivedIndices;
TYPED_TEST_CASE(MultiColumnIndexTest, DerivedIndices);

TYPED_TEST(MultiColumnIndexTest, FullRange) {
  auto begin_int_str = this->index_int_str->cbegin();
  auto end_int_str = this->index_int_str->cend();
  auto result_values_int_str =
      this->result_as_vector({this->dict_col_int, this->dict_col_str}, begin_int_str, end_int_str);
  auto expected_values_int_str =
      std::vector<std::vector<AllTypeVariant>>{{0, "baz"}, {1, "baz"}, {2, "bar"}, {3, "foo"}, {4, "bar"},
                                               {4, "bar"}, {4, "foo"}, {7, "baz"}, {8, "foo"}, {9, "foo"}};
  EXPECT_EQ(expected_values_int_str, result_values_int_str);

  auto begin_str_int = this->index_str_int->cbegin();
  auto end_str_int = this->index_str_int->cend();
  auto result_values_str_int =
      this->result_as_vector({this->dict_col_str, this->dict_col_int}, begin_str_int, end_str_int);
  auto expected_values_str_int =
      std::vector<std::vector<AllTypeVariant>>{{"bar", 2}, {"bar", 4}, {"bar", 4}, {"baz", 0}, {"baz", 1},
                                               {"baz", 7}, {"foo", 3}, {"foo", 4}, {"foo", 8}, {"foo", 9}};
  EXPECT_EQ(expected_values_str_int, result_values_str_int);
}

TYPED_TEST(MultiColumnIndexTest, PointQueryWithSingleReturnValue) {
  auto begin = this->index_int_str->lower_bound({3, "foo"});
  auto end = this->index_int_str->upper_bound({3, "foo"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{0};

  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, PointQueryWithNoReturnValue) {
  auto begin = this->index_int_str->lower_bound({3, "hello"});
  auto end = this->index_int_str->upper_bound({3, "hello"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, PointQueryWithMultipleReturnValues) {
  auto begin = this->index_int_str->lower_bound({4, "bar"});
  auto end = this->index_int_str->upper_bound({4, "bar"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{1, 7};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQuery) {
  auto begin = this->index_int_str->lower_bound({1, "baz"});
  auto end = this->index_int_str->upper_bound({3, "bar"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{4, 8};

  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryBelow) {
  auto begin = this->index_int_str->lower_bound({-3, "arrr!"});
  auto end = this->index_int_str->upper_bound({0, "bar"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryPartiallyBelow) {
  auto begin = this->index_int_str->lower_bound({-3, "arrr!"});
  auto end = this->index_int_str->upper_bound({1, "baz"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{2, 8};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryAbove) {
  auto begin = this->index_int_str->lower_bound({10, "srsly?"});
  auto end = this->index_int_str->upper_bound({13, "srsly?"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryPartiallyAbove) {
  auto begin = this->index_int_str->lower_bound({8, "bar"});
  auto end = this->index_int_str->upper_bound({13, "srsly?"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryOpenEnd) {
  auto begin = this->index_int_str->lower_bound({8, "bar"});
  auto end = this->index_int_str->cend();

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, RangeQueryOpenBegin) {
  auto begin = this->index_int_str->cbegin();
  auto end = this->index_int_str->upper_bound({1, "baz"});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{2, 8};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, TooManyReferenceValues) {
  if (!IS_DEBUG) return;
  EXPECT_THROW(this->index_int_str->lower_bound({1, "baz", 3.0f}), std::logic_error);
  EXPECT_THROW(this->index_int_str->upper_bound({1, "baz", 3.0f}), std::logic_error);
}

TYPED_TEST(MultiColumnIndexTest, QueryWithFewerValuesThanColumns) {
  auto begin = this->index_int_str->lower_bound({4});
  auto end = this->index_int_str->upper_bound({4});

  auto result = std::set<size_t>(begin, end);
  auto expected = std::set<size_t>{1, 3, 7};

  EXPECT_EQ(expected, result);
}

TYPED_TEST(MultiColumnIndexTest, IsIndexForTest) {
  EXPECT_TRUE(this->index_int_str->is_index_for({this->dict_col_int}));
  EXPECT_TRUE(this->index_int_str->is_index_for({this->dict_col_int, this->dict_col_str}));
  EXPECT_TRUE(this->index_str_int->is_index_for({this->dict_col_str}));
  EXPECT_TRUE(this->index_str_int->is_index_for({this->dict_col_str, this->dict_col_int}));

  EXPECT_FALSE(this->index_int_str->is_index_for({this->dict_col_str, this->dict_col_int}));
  EXPECT_FALSE(this->index_str_int->is_index_for({this->dict_col_int}));
  EXPECT_FALSE(this->index_str_int->is_index_for({this->dict_col_int, this->dict_col_str}));
  EXPECT_FALSE(this->index_str_int->is_index_for({}));
}

TYPED_TEST(MultiColumnIndexTest, CreateAndRetrieveUsingChunk) {
  auto chunk = std::make_shared<Chunk>(ChunkColumns({this->dict_col_int, this->dict_col_str}));

  chunk->create_index<TypeParam>({this->dict_col_int});
  chunk->create_index<TypeParam>({this->dict_col_int, this->dict_col_str});

  auto indices_int = chunk->get_indices({this->dict_col_int});
  auto indices_int_str = chunk->get_indices({this->dict_col_int, this->dict_col_str});
  auto indices_str = chunk->get_indices({this->dict_col_str});

  EXPECT_EQ(2u, indices_int.size());
  EXPECT_EQ(1u, indices_int_str.size());
  EXPECT_EQ(0u, indices_str.size());

  EXPECT_TRUE(indices_int[0]->is_index_for({this->dict_col_int}));
  EXPECT_TRUE(indices_int[1]->is_index_for({this->dict_col_int}));
  EXPECT_TRUE(indices_int_str[0]->is_index_for({this->dict_col_int, this->dict_col_str}));
}

}  // namespace opossum
