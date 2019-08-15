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
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

template <typename DerivedIndex>
class SingleSegmentIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // Int segments
    dict_segment_int_no_nulls =
        BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {3, 4, 0, 4, 2, 7, 8, 1, 4, 9});
    dict_segment_int_no_nulls_2 = BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {3, 4, 0});
    // dict_segment_int_nulls = BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {NULL_VALUE, NULL_VALUE, NULL_VALUE});
    dict_segment_int_empty = BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {});

    // Long segments
    dict_segment_long_no_nulls =
        BaseTest::create_dict_segment_by_type<int64_t>(DataType::Long, {3, 4, 0, 4, 2, 7, 8, 1, 4, 9});
    dict_segment_long_empty = BaseTest::create_dict_segment_by_type<int64_t>(DataType::Long, {});

    // Float segments
    dict_segment_float_no_nulls = BaseTest::create_dict_segment_by_type<float>(
        DataType::Float, {3.1f, 4.9f, 0.2f, 4.8f, 2.3f, 7.7f, 8.4f, 1.6f, 4.5f, 9.0f});
    dict_segment_float_empty = BaseTest::create_dict_segment_by_type<float>(DataType::Float, {});

    // Double segments
    dict_segment_double_no_nulls = BaseTest::create_dict_segment_by_type<double>(
        DataType::Double, {3.1, 4.9, 0.2, 4.8, 2.3, 7.7, 8.4, 1.6, 4.5, 9.0});
    dict_segment_double_empty = BaseTest::create_dict_segment_by_type<double>(DataType::Double, {});

    // String segments
    dict_segment_str_no_nulls = BaseTest::create_dict_segment_by_type<pmr_string>(
        DataType::String, {"hello", "world", "test", "foo", "bar", "foo"});
    dict_segment_str_empty = BaseTest::create_dict_segment_by_type<pmr_string>(DataType::String, {});

    // Int indexes
    index_int_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_no_nulls}));
    index_int_no_nulls_2 =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_no_nulls}));
    index_int_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_empty}));
    // Long indexes
    index_long_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_no_nulls}));
    index_long_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_empty}));
    // Float indexes
    index_float_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_no_nulls}));
    index_float_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_empty}));
    // Double indexes
    index_double_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_no_nulls}));
    index_double_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_empty}));
    // String indexes
    index_str_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_str_no_nulls}));
    index_str_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_str_empty}));
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

  // segments
  std::shared_ptr<BaseSegment> dict_segment_int_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_int_no_nulls_2 = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_int_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_long_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_float_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_double_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_str_no_nulls = nullptr;

  std::shared_ptr<BaseSegment> dict_segment_int_empty = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_long_empty = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_float_empty = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_double_empty = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_str_empty = nullptr;

  // indexes
  std::shared_ptr<AbstractIndex> index_int_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_int_no_nulls_2 = nullptr;
  std::shared_ptr<AbstractIndex> index_long_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_float_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_double_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_str_no_nulls = nullptr;

  std::shared_ptr<AbstractIndex> index_int_empty = nullptr;
  std::shared_ptr<AbstractIndex> index_long_empty = nullptr;
  std::shared_ptr<AbstractIndex> index_float_empty = nullptr;
  std::shared_ptr<AbstractIndex> index_double_empty = nullptr;
  std::shared_ptr<AbstractIndex> index_str_empty = nullptr;
  std::shared_ptr<AbstractIndex> no_segment_index = nullptr;
};

// List of indexes to test
// TODO(Marcel) creating BTreeIndex for empty segments fails
typedef ::testing::Types</*AdaptiveRadixTreeIndex, BTreeIndex, CompositeGroupKeyIndex,*/
                         GroupKeyIndex /* add further indexes */>
    DerivedIndexes;
TYPED_TEST_CASE(SingleSegmentIndexTest, DerivedIndexes, );  // NOLINT(whitespace/parens)

/*
  bool is_index_for(const std::vector<std::shared_ptr<const BaseSegment>>& segments) const;
  
  | Characteristic     |   Block 1  | Block 2 | Block 3 | Block 4 | Block 5 |
  |--------------------|------------|---------|---------|---------|---------|
  |[A] value data type |        Int |    Long |   Float |  Double |  String |
  |[B] index empty     |       true |   false |
  |[C] seg. empty      |       true |   false |
  |[D] seg. coverage   |       true |   false |
  |[E] segments emtpy  |       true |   false |
  |[F] seg. type       | Dict. Seg. |

  Base Choice: 
    A1, B2, C2, D1, E2, F1
  Further derived combinations:
    A2, B2, C2, D1, E2, F1
    A3, B2, C2, D1, E2, F1
    A4, B2, C2, D1, E2, F1
    A5, B2, C2, D1, E2, F1
    A1, B1, C1, D1, E2, F1
    A1, B2, C2, D2, E2, F1
    A1, B2, C2, D1, E1, F1
*/

TYPED_TEST(SingleSegmentIndexTest, IsIndexForTest) {
  // A1, B2, C2, D1, E2, F1
  EXPECT_TRUE(this->index_int_no_nulls->is_index_for({this->dict_segment_int_no_nulls}));
  // A2, B2, C2, D1, E2, F1
  EXPECT_TRUE(this->index_long_no_nulls->is_index_for({this->dict_segment_long_no_nulls}));
  // A3, B2, C2, D1, E2, F1
  EXPECT_TRUE(this->index_float_no_nulls->is_index_for({this->dict_segment_float_no_nulls}));
  // A4, B2, C2, D1, E2, F1
  EXPECT_TRUE(this->index_double_no_nulls->is_index_for({this->dict_segment_double_no_nulls}));
  // A5, B2, C2, D1, E2, F1
  EXPECT_TRUE(this->index_str_no_nulls->is_index_for({this->dict_segment_str_no_nulls}));
  // A1, B1, C1, D1, E2, F1
  EXPECT_TRUE(this->index_int_empty->is_index_for({this->dict_segment_int_empty}));
  // A1, B2, C2, D2, E2, F1
  EXPECT_FALSE(this->index_int_no_nulls->is_index_for({this->dict_segment_int_no_nulls_2}));
  // A1, B2, C2, D1, E1, F1
  EXPECT_FALSE(this->index_int_empty->is_index_for({}));
  // other
  EXPECT_FALSE(this->index_str_no_nulls->is_index_for({this->dict_segment_int_no_nulls}));
  EXPECT_FALSE(
      this->index_str_no_nulls->is_index_for({this->dict_segment_str_no_nulls, this->dict_segment_int_no_nulls}));
}

/*
  Iterator lower_bound(const std::vector<AllTypeVariant>& values) const;
  Iterator upper_bound(const std::vector<AllTypeVariant>& values) const;
  Iterator cbegin() const;
  Iterator cend() const;
  Iterator null_cbegin() const;
  Iterator null_cend() const;

  | Characteristic      |       Block 1  |        Block 2 |        Block 3 |          Block 4 | Block 5 |
  |---------------------|----------------|----------------|----------------|------------------|---------|
  |[A] value data type  |            Int |           Long |          Float |           Double |  String |     
  |[B] value type       |           null | smallest value | largest value  | non-edge value   |    none |
  |                     |                | in segment     | in segment     | (either smallest |
  |                     |                |                |                | nor largest)     |
  |[C] value is in seg. |           true |          false |
  |[D] values is empty  |           true |          false |

  Base Choice:
    A4, B4, C1, D2
  Further derived combinations:
   (A4, B4, C1, D1) --infeasible----+
    A4, B5, C2, D1 <--alternative-<-+
    A4, B4, C2, D2                  |
    A4, B1, C1, D2                  |
    A4, B2, C1, D2                  |
    A4, B3, C1, D2                  |
    A4, B5, C1, D2 --infeasible-----+
    A1, B4, C1, D2
    A2, B4, C1, D2
    A3, B4, C1, D2
    A5, B4, C1, D2

*/

TYPED_TEST(SingleSegmentIndexTest, LowerBoundTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, UpperBoundTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, CBeginTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, CEndTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, NullCBeginTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, NullCEndTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, SegmentIndexTypeTest) {
  if constexpr (std::is_same_v<TypeParam, AdaptiveRadixTreeIndex>) {
    EXPECT_EQ(this->index_int_no_nulls->type(), SegmentIndexType::AdaptiveRadixTree);
  } else if constexpr (std::is_same_v<TypeParam, BTreeIndex>) {
    EXPECT_EQ(this->index_int_no_nulls->type(), SegmentIndexType::BTree);
  } else if constexpr (std::is_same_v<TypeParam, CompositeGroupKeyIndex>) {
    EXPECT_EQ(this->index_int_no_nulls->type(), SegmentIndexType::CompositeGroupKey);
  } else if constexpr (std::is_same_v<TypeParam, GroupKeyIndex>) {
    EXPECT_EQ(this->index_int_no_nulls->type(), SegmentIndexType::GroupKey);
  }
}

/*
  size_t memory_consumption() const;
  
  |    Characteristic               | Block 1 | Block 2 |
  |---------------------------------|---------|---------|
  |[A] index is empty               |    true |   false |
  |[B] index has null positions     |    true |   false |
  |[C] index has non-null positions |    true |   false |
  
  Base Choice:
    A2, B1, C1
  Further derived combinations:
    A2, B1, C2
    A2, B2, C1
   (A1, B1, C1) --infeasible---+
    A1, B2, C2 <-alternative-<-+
*/

TYPED_TEST(SingleSegmentIndexTest, MemoryConsumptionTest) {
  // TODO(Marcel) implement (Refine combinations of blocks into test values)
}

TYPED_TEST(SingleSegmentIndexTest, CreateZeroSegmentIndexTest) {
  EXPECT_THROW(std::make_shared<TypeParam>(std::vector<std::shared_ptr<const BaseSegment>>({})), std::logic_error);
}

TYPED_TEST(SingleSegmentIndexTest, FullRange) {
  auto begin_int = this->index_int_no_nulls->cbegin();
  auto end_int = this->index_int_no_nulls->cend();
  auto result_values_int = this->result_as_vector(this->dict_segment_int_no_nulls, begin_int, end_int);
  auto expected_values_int = std::vector<AllTypeVariant>{0, 1, 2, 3, 4, 4, 4, 7, 8, 9};
  EXPECT_EQ(expected_values_int, result_values_int);

  auto begin_str = this->index_str_no_nulls->cbegin();
  auto end_str = this->index_str_no_nulls->cend();
  auto result_values_str = this->result_as_vector(this->dict_segment_str_no_nulls, begin_str, end_str);
  auto expected_values_str = std::vector<AllTypeVariant>{"bar", "foo", "foo", "hello", "test", "world"};
  EXPECT_EQ(expected_values_str, result_values_str);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithSingleReturnValue) {
  auto begin = this->index_int_no_nulls->lower_bound({3});
  auto end = this->index_int_no_nulls->upper_bound({3});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{0};

  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithNoReturnValue) {
  auto begin = this->index_int_no_nulls->lower_bound({5});
  auto end = this->index_int_no_nulls->upper_bound({5});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, PointQueryWithMultipleReturnValues) {
  auto begin = this->index_int_no_nulls->lower_bound({4});
  auto end = this->index_int_no_nulls->upper_bound({4});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{1, 3, 8};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQuery) {
  auto begin = this->index_int_no_nulls->lower_bound({1});
  auto end = this->index_int_no_nulls->upper_bound({3});

  auto result_positions = std::set<std::size_t>(begin, end);
  auto result_values = this->result_as_vector(this->dict_segment_int_no_nulls, begin, end);

  auto expected_positions = std::set<std::size_t>{0, 4, 7};
  auto expected_values = std::vector<AllTypeVariant>{1, 2, 3};

  EXPECT_EQ(expected_positions, result_positions);
  EXPECT_EQ(expected_values, result_values);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryBelow) {
  auto begin = this->index_int_no_nulls->lower_bound({-3});
  auto end = this->index_int_no_nulls->upper_bound({-1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryPartiallyBelow) {
  auto begin = this->index_int_no_nulls->lower_bound({-3});
  auto end = this->index_int_no_nulls->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryAbove) {
  auto begin = this->index_int_no_nulls->lower_bound({10});
  auto end = this->index_int_no_nulls->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryPartiallyAbove) {
  auto begin = this->index_int_no_nulls->lower_bound({8});
  auto end = this->index_int_no_nulls->upper_bound({13});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryOpenEnd) {
  auto begin = this->index_int_no_nulls->lower_bound({8});
  auto end = this->index_int_no_nulls->cend();

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{6, 9};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, RangeQueryOpenBegin) {
  auto begin = this->index_int_no_nulls->cbegin();
  auto end = this->index_int_no_nulls->upper_bound({1});

  auto result = std::set<std::size_t>(begin, end);
  auto expected = std::set<std::size_t>{2, 7};
  EXPECT_EQ(expected, result);
}

TYPED_TEST(SingleSegmentIndexTest, IndexOnNonDictionaryThrows) {
  if (!HYRISE_DEBUG) GTEST_SKIP();
  auto vs_int = std::make_shared<ValueSegment<int>>();
  vs_int->append(4);

  EXPECT_THROW(std::make_shared<TypeParam>(std::vector<std::shared_ptr<const BaseSegment>>({vs_int})),
               std::logic_error);
}

}  // namespace opossum
