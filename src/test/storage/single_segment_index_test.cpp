#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class SingleSegmentIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    dict_segment_int = BaseTest::create_dict_segment_by_type<int>(DataType::Int, {3, 4, 0, 4, 2, 7, 8, 1, 4, 9});
    index_int = std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int}));

    dict_segment_str = BaseTest::create_dict_segment_by_type<std::string>(
        DataType::String, {"hello", "world", "test", "foo", "bar", "foo"});
    index_str = std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_str}));
  }

  template <class Iterator>
  static std::vector<AllTypeVariant> result_as_vector(std::shared_ptr<BaseSegment> segment, Iterator begin,
                                                      Iterator end) {
    std::vector<AllTypeVariant> result{};
    for (auto iter(std::move(begin)); iter != end; ++iter) {
      result.emplace_back((*segment)[*iter]);
    }
    return result;
  }

  std::shared_ptr<BaseIndex> index_int = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_int = nullptr;
  std::shared_ptr<BaseIndex> index_str = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_str = nullptr;
};

// List of indices to test
typedef ::testing::Types<GroupKeyIndex, CompositeGroupKeyIndex, AdaptiveRadixTreeIndex /* add further indices */>
    DerivedIndices;
TYPED_TEST_CASE(SingleSegmentIndexTest, DerivedIndices);

TYPED_TEST(SingleSegmentIndexTest, FullRange) {
  auto begin_int = this->index_int->cbegin();
  auto end_int = this->index_int->cend();
  auto result_values_int = this->result_as_vector(this->dict_segment_int, begin_int, end_int);
  auto expected_values_int = std::vector<AllTypeVariant>{0, 1, 2, 3, 4, 4, 4, 7, 8, 9};
  EXPECT_EQ(expected_values_int, result_values_int);

  auto begin_str = this->index_str->cbegin();
  auto end_str = this->index_str->cend();
  auto result_values_str = this->result_as_vector(this->dict_segment_str, begin_str, end_str);
  auto expected_values_str = std::vector<AllTypeVariant>{"bar", "foo", "foo", "hello", "test", "world"};
  EXPECT_EQ(expected_values_str, result_values_str);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithSingleReturnValue) {
  auto begin = this->index_int->lower_bound({3});
  auto end = this->index_int->upper_bound({3});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{0};

  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithNoReturnValue) {
  auto begin = this->index_int->lower_bound({5});
  auto end = this->index_int->upper_bound({5});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithMultipleReturnValues) {
  auto begin = this->index_int->lower_bound({4});
  auto end = this->index_int->upper_bound({4});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{1, 3, 8};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQuery) {
  auto begin = this->index_int->lower_bound({1});
  auto end = this->index_int->upper_bound({3});

  auto result_positions = std::set<std::size_t>(begin, end);
  auto result_values = this->result_as_vector(this->dict_segment_int, begin, end);

  auto expected_positions = std::set<std::size_t>{0, 4, 7};
  auto expected_values = std::vector<AllTypeVariant>{1, 2, 3};

  EXPECT_EQ(expected_positions, result_positions);
  EXPECT_EQ(expected_values, result_values);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryBelow) {
  auto begin = this->index_int->lower_bound({-3});
  auto end = this->index_int->upper_bound({-1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryPartiallyBelow) {
  auto begin = this->index_int->lower_bound({-3});
  auto end = this->index_int->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryAbove) {
  auto begin = this->index_int->lower_bound({10});
  auto end = this->index_int->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryPartiallyAbove) {
  auto begin = this->index_int->lower_bound({8});
  auto end = this->index_int->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryOpenEnd) {
  auto begin = this->index_int->lower_bound({8});
  auto end = this->index_int->cend();

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryOpenBegin) {
  auto begin = this->index_int->cbegin();
  auto end = this->index_int->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, IsIndexForTest) {
  EXPECT_TRUE(this->index_int->is_index_for({this->dict_segment_int}));
  EXPECT_TRUE(this->index_str->is_index_for({this->dict_segment_str}));

  EXPECT_FALSE(this->index_int->is_index_for({this->dict_segment_str}));
  EXPECT_FALSE(this->index_str->is_index_for({this->dict_segment_int}));
  EXPECT_FALSE(this->index_str->is_index_for({this->dict_segment_str, this->dict_segment_int}));
  EXPECT_FALSE(this->index_str->is_index_for({}));
}

TYPED_TEST(SingleSegmentIndexTest, IndexOnNonDictionaryThrows) {
  if (!IS_DEBUG) return;
  auto vs_int = std::make_shared<ValueSegment<int>>();
  vs_int->append(4);

  EXPECT_THROW(std::make_shared<TypeParam>(std::vector<std::shared_ptr<const BaseSegment>>({vs_int})),
               std::logic_error);
}

}  // namespace opossum
