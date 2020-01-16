#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"

#include "storage/base_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/index/adaptive_radix_tree/adaptive_radix_tree_index.hpp"
#include "storage/index/b_tree/b_tree_index.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "resolve_type.hpp"
#include "types.hpp"

// In this domain input modeling is explicitly used.
// https://github.com/hyrise/hyrise/wiki/Input-Domain-Modeling

namespace opossum {

template <typename DerivedIndex>
class SingleSegmentIndexTest : public BaseTest {
 protected:
  void SetUp() override {
    // Int segments
    dict_segment_int_no_nulls =
        BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {3, 4, 0, 4, 2, 7, 8, 1, 4, 9});
    dict_segment_int_no_nulls_2 = BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {3, 4, 0});
    dict_segment_int_nulls =
        BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {std::nullopt, std::nullopt, std::nullopt});
    dict_segment_int_mixed = BaseTest::create_dict_segment_by_type<int32_t>(
        DataType::Int, {std::nullopt, 3, std::nullopt, 0, std::nullopt, 4, 3, std::nullopt});
    dict_segment_int_empty = BaseTest::create_dict_segment_by_type<int32_t>(DataType::Int, {});

    // Long segments
    dict_segment_long_no_nulls =
        BaseTest::create_dict_segment_by_type<int64_t>(DataType::Long, {3L, 4L, 0L, 4L, 2L, 7L, 8L, 1L, 4L, 9L});
    dict_segment_long_nulls =
        BaseTest::create_dict_segment_by_type<int64_t>(DataType::Long, {std::nullopt, std::nullopt, std::nullopt});
    dict_segment_long_mixed = BaseTest::create_dict_segment_by_type<int64_t>(
        DataType::Long, {std::nullopt, 3L, std::nullopt, 0L, std::nullopt, 4L, 3L, std::nullopt});
    dict_segment_long_empty = BaseTest::create_dict_segment_by_type<int64_t>(DataType::Long, {});

    // Float segments
    dict_segment_float_no_nulls = BaseTest::create_dict_segment_by_type<float>(
        DataType::Float, {3.1f, 4.9f, 0.2f, 4.8f, 2.3f, 7.7f, 8.4f, 1.6f, 4.5f, 9.0f});
    dict_segment_float_nulls =
        BaseTest::create_dict_segment_by_type<float>(DataType::Float, {std::nullopt, std::nullopt, std::nullopt});
    dict_segment_float_mixed = BaseTest::create_dict_segment_by_type<float>(
        DataType::Float, {std::nullopt, 3.1f, std::nullopt, 0.2f, std::nullopt, 4.8f, 3.1f, std::nullopt});
    dict_segment_float_empty = BaseTest::create_dict_segment_by_type<float>(DataType::Float, {});

    // Double segments
    dict_segment_double_no_nulls = BaseTest::create_dict_segment_by_type<double>(
        DataType::Double, {3.1, 4.9, 0.2, 4.8, 2.3, 7.7, 8.4, 1.6, 4.5, 9.0});
    dict_segment_double_nulls =
        BaseTest::create_dict_segment_by_type<double>(DataType::Double, {std::nullopt, std::nullopt, std::nullopt});
    dict_segment_double_mixed = BaseTest::create_dict_segment_by_type<double>(
        DataType::Double, {std::nullopt, 3.1, std::nullopt, 0.2, std::nullopt, 4.8, 3.1, std::nullopt});
    dict_segment_double_empty = BaseTest::create_dict_segment_by_type<double>(DataType::Double, {});

    // String segments
    dict_segment_string_no_nulls = BaseTest::create_dict_segment_by_type<pmr_string>(
        DataType::String, {"hello", "world", "test", "foo", "bar", "foo"});
    dict_segment_string_nulls =
        BaseTest::create_dict_segment_by_type<pmr_string>(DataType::String, {std::nullopt, std::nullopt, std::nullopt});
    dict_segment_string_mixed = BaseTest::create_dict_segment_by_type<pmr_string>(
        DataType::String, {std::nullopt, "hello", std::nullopt, "alpha", std::nullopt, "test", "hello", std::nullopt});
    dict_segment_string_empty = BaseTest::create_dict_segment_by_type<pmr_string>(DataType::String, {});

    // Int indexes
    index_int_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_no_nulls}));
    index_int_no_nulls_2 =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_no_nulls}));
    index_int_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_nulls}));
    index_int_mixed =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_mixed}));
    index_int_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_int_empty}));
    // Long indexes
    index_long_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_no_nulls}));
    index_long_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_nulls}));
    index_long_mixed =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_mixed}));
    index_long_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_long_empty}));
    // Float indexes
    index_float_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_no_nulls}));
    index_float_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_nulls}));
    index_float_mixed =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_mixed}));
    index_float_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_float_empty}));
    // Double indexes
    index_double_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_no_nulls}));
    index_double_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_nulls}));
    index_double_mixed =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_mixed}));
    index_double_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_double_empty}));
    // String indexes
    index_string_no_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_no_nulls}));
    index_string_nulls =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_nulls}));
    index_string_mixed =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_mixed}));
    index_string_empty =
        std::make_shared<DerivedIndex>(std::vector<std::shared_ptr<const BaseSegment>>({dict_segment_string_empty}));
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
  std::shared_ptr<BaseSegment> dict_segment_int_mixed = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_int_empty = nullptr;

  std::shared_ptr<BaseSegment> dict_segment_long_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_long_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_long_mixed = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_long_empty = nullptr;

  std::shared_ptr<BaseSegment> dict_segment_float_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_float_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_float_mixed = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_float_empty = nullptr;

  std::shared_ptr<BaseSegment> dict_segment_double_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_double_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_double_mixed = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_double_empty = nullptr;

  std::shared_ptr<BaseSegment> dict_segment_string_no_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_string_nulls = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_string_mixed = nullptr;
  std::shared_ptr<BaseSegment> dict_segment_string_empty = nullptr;

  // indexes
  std::shared_ptr<AbstractIndex> index_int_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_int_no_nulls_2 = nullptr;
  std::shared_ptr<AbstractIndex> index_int_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_int_mixed = nullptr;
  std::shared_ptr<AbstractIndex> index_int_empty = nullptr;

  std::shared_ptr<AbstractIndex> index_long_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_long_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_long_mixed = nullptr;
  std::shared_ptr<AbstractIndex> index_long_empty = nullptr;

  std::shared_ptr<AbstractIndex> index_float_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_float_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_float_mixed = nullptr;
  std::shared_ptr<AbstractIndex> index_float_empty = nullptr;

  std::shared_ptr<AbstractIndex> index_double_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_double_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_double_mixed = nullptr;
  std::shared_ptr<AbstractIndex> index_double_empty = nullptr;

  std::shared_ptr<AbstractIndex> index_string_no_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_string_nulls = nullptr;
  std::shared_ptr<AbstractIndex> index_string_mixed = nullptr;
  std::shared_ptr<AbstractIndex> index_string_empty = nullptr;

  // index NULL positions
  std::vector<ChunkOffset>* index_int_no_nulls_null_positions;
  std::vector<ChunkOffset>* index_int_no_nulls_2_null_positions;
  std::vector<ChunkOffset>* index_int_nulls_null_positions;
  std::vector<ChunkOffset>* index_int_mixed_null_positions;
  std::vector<ChunkOffset>* index_int_empty_null_positions;

  std::vector<ChunkOffset>* index_long_no_nulls_null_positions;
  std::vector<ChunkOffset>* index_long_nulls_null_positions;
  std::vector<ChunkOffset>* index_long_mixed_null_positions;
  std::vector<ChunkOffset>* index_long_empty_null_positions;

  std::vector<ChunkOffset>* index_float_no_nulls_null_positions;
  std::vector<ChunkOffset>* index_float_nulls_null_positions;
  std::vector<ChunkOffset>* index_float_mixed_null_positions;
  std::vector<ChunkOffset>* index_float_empty_null_positions;

  std::vector<ChunkOffset>* index_double_no_nulls_null_positions;
  std::vector<ChunkOffset>* index_double_nulls_null_positions;
  std::vector<ChunkOffset>* index_double_mixed_null_positions;
  std::vector<ChunkOffset>* index_double_empty_null_positions;

  std::vector<ChunkOffset>* index_string_no_nulls_null_positions;
  std::vector<ChunkOffset>* index_string_nulls_null_positions;
  std::vector<ChunkOffset>* index_string_mixed_null_positions;
  std::vector<ChunkOffset>* index_string_empty_null_positions;
};

// List of indexes to test
typedef ::testing::Types<AdaptiveRadixTreeIndex, BTreeIndex, /*CompositeGroupKeyIndex,*/ GroupKeyIndex> DerivedIndexes;
TYPED_TEST_SUITE(SingleSegmentIndexTest, DerivedIndexes, );  // NOLINT(whitespace/parens)

/*
  Test cases:
    IsIndexForTest
  Tested functions:
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
  EXPECT_TRUE(this->index_string_no_nulls->is_index_for({this->dict_segment_string_no_nulls}));
  // A1, B1, C1, D1, E2, F1
  EXPECT_TRUE(this->index_int_empty->is_index_for({this->dict_segment_int_empty}));
  // A1, B2, C2, D2, E2, F1
  EXPECT_FALSE(this->index_int_no_nulls->is_index_for({this->dict_segment_int_no_nulls_2}));
  // A1, B2, C2, D1, E1, F1
  EXPECT_FALSE(this->index_int_empty->is_index_for({}));
  // other
  EXPECT_FALSE(this->index_string_no_nulls->is_index_for({this->dict_segment_int_no_nulls}));
  EXPECT_FALSE(
      this->index_string_no_nulls->is_index_for({this->dict_segment_string_no_nulls, this->dict_segment_int_no_nulls}));
}

/*
  Test cases:
    LowerBoundTest
    UpperBoundTest
  Tested functions:
    Iterator lower_bound(const std::vector<AllTypeVariant>& values) const;
    Iterator upper_bound(const std::vector<AllTypeVariant>& values) const;

  | Characteristic      |       Block 1  |        Block 2 |        Block 3 |          Block 4 | Block 5 |
  |---------------------|----------------|----------------|----------------|------------------|---------|
  |[A] value data type  |            Int |           Long |          Float |           Double |  String |     
  |[B] value type       |           NULL | smallest value | largest value  | non-edge value   |    none |
  |                     |                | in segment     | in segment     | (either smallest |
  |                     |                |                |                | nor largest)     |
  |[C] value is in seg. |           true |          false |
  |[D] values is empty  |           true |          false |

  Base Choice:
    A1, B4, C1, D2
  Further derived Base Choice combinations:
   (A1, B4, C1, D1) --infeasible----+
    A1, B5, C2, D1 <--alternative-<-+
    A1, B4, C2, D2                  |
    A1, B1, C1, D2                  |
    A1, B2, C1, D2                  |
    A1, B3, C1, D2                  |
   (A1, B5, C1, D2) --infeasible----+
    A2, B4, C1, D2
    A3, B4, C1, D2
    A4, B4, C1, D2
    A5, B4, C1, D2
*/

TYPED_TEST(SingleSegmentIndexTest, LowerBoundTest) {
  // A1, B4, C1, D2
  EXPECT_EQ(this->index_int_mixed->lower_bound({3}), this->index_int_mixed->cbegin() + 1);
  // A1, B5, C2, D1
  EXPECT_THROW(this->index_int_mixed->lower_bound({}), std::logic_error);
  // A1, B4, C2, D2
  EXPECT_EQ(this->index_int_mixed->lower_bound({2}), this->index_int_mixed->cbegin() + 1);
  // A1, B1, C1, D2
  EXPECT_THROW(this->index_int_mixed->lower_bound({NULL_VALUE}), std::logic_error);
  // A1, B2, C1, D2
  EXPECT_EQ(this->index_int_mixed->lower_bound({0}), this->index_int_mixed->cbegin());
  // A1, B3, C1, D2
  EXPECT_EQ(this->index_int_mixed->lower_bound({4}), this->index_int_mixed->cbegin() + 3);
  // A2, B4, C1, D2
  EXPECT_EQ(this->index_long_mixed->lower_bound({static_cast<int64_t>(3)}), this->index_long_mixed->cbegin() + 1);
  // A3, B4, C1, D2
  EXPECT_EQ(this->index_float_mixed->lower_bound({3.1f}), this->index_float_mixed->cbegin() + 1);
  // A4, B4, C1, D2
  EXPECT_EQ(this->index_double_mixed->lower_bound({3.1}), this->index_double_mixed->cbegin() + 1);
  // A5, B4, C1, D2
  EXPECT_EQ(this->index_string_mixed->lower_bound({"hello"}), this->index_string_mixed->cbegin() + 1);
}

TYPED_TEST(SingleSegmentIndexTest, UpperBoundTest) {
  // A1, B4, C1, D2
  EXPECT_EQ(this->index_int_mixed->upper_bound({3}), this->index_int_mixed->cbegin() + 3);
  // A1, B5, C2, D1
  EXPECT_THROW(this->index_int_mixed->upper_bound({}), std::logic_error);
  // A1, B4, C2, D2
  EXPECT_EQ(this->index_int_mixed->upper_bound({2}), this->index_int_mixed->cbegin() + 1);
  // A1, B1, C1, D2
  EXPECT_THROW(this->index_int_mixed->upper_bound({NULL_VALUE}), std::logic_error);
  // A1, B2, C1, D2
  EXPECT_EQ(this->index_int_mixed->upper_bound({0}), this->index_int_mixed->cbegin() + 1);
  // A1, B3, C1, D2
  EXPECT_EQ(this->index_int_mixed->upper_bound({4}), this->index_int_mixed->cbegin() + 4);
  // A2, B4, C1, D2
  EXPECT_EQ(this->index_long_mixed->upper_bound({static_cast<int64_t>(3)}), this->index_long_mixed->cbegin() + 3);
  // A3, B4, C1, D2
  EXPECT_EQ(this->index_float_mixed->upper_bound({3.1f}), this->index_float_mixed->cbegin() + 3);
  // A4, B4, C1, D2
  EXPECT_EQ(this->index_double_mixed->upper_bound({3.1}), this->index_double_mixed->cbegin() + 3);
  // A5, B4, C1, D2
  EXPECT_EQ(this->index_string_mixed->upper_bound({"hello"}), this->index_string_mixed->cbegin() + 3);
}

/*
  Test cases:
    CBeginCEndTest
    NullCBeginCEndTest

  Tested functions:
    Iterator cbegin() const;
    Iterator cend() const;
    Iterator null_cbegin() const;
    Iterator null_cend() const;

  | Characteristic      | Block 1  | Block 2 | Block 3 | Block 4 | Block 5 |
  |---------------------|----------|---------|---------|---------|---------|
  |[A] value data type  |      Int |    Long |   Float |  Double |  String |     
  |[B] nulls only       |     true |   false |
  |[C] non-NULLs only   |     true |   false |
  |[D] index is empty   |     true |   false |

  Base Choice:
    A1, B2, C2, D2
  Further derived Base Choice combinations:
    A1, B2, C2, D1
    A1, B2, C1, D2
    A1, B1, C2, D2
    A2, B2, C2, D2
    A3, B2, C2, D2
    A4, B2, C2, D2
    A5, B2, C2, D2

*/

TYPED_TEST(SingleSegmentIndexTest, CBeginCEndTest) {
  // A1, B1, C2, D2
  EXPECT_EQ(this->index_int_no_nulls->cend(), this->index_int_no_nulls->cbegin() + 10u);
  // A1, B2, C1, D2
  EXPECT_EQ(this->index_int_nulls->cend(), this->index_int_nulls->cbegin() + 0u);
  // A1, B2, C2, D2
  EXPECT_EQ(this->index_int_mixed->cend(), this->index_int_mixed->cbegin() + 4u);
  // A1, B2, D2, D1
  EXPECT_EQ(this->index_int_empty->cend(), this->index_int_empty->cbegin() + 0u);

  // A2, B1, C2, D2
  EXPECT_EQ(this->index_long_no_nulls->cend(), this->index_long_no_nulls->cbegin() + 10u);
  // A2, B2, C1, D2
  EXPECT_EQ(this->index_long_nulls->cend(), this->index_long_nulls->cbegin() + 0u);
  // A2, B2, C2, D2
  EXPECT_EQ(this->index_long_mixed->cend(), this->index_long_mixed->cbegin() + 4u);
  // A2, B2, D2, D1
  EXPECT_EQ(this->index_long_empty->cend(), this->index_long_empty->cbegin() + 0u);

  // A3, B1, C2, D2
  EXPECT_EQ(this->index_float_no_nulls->cend(), this->index_float_no_nulls->cbegin() + 10u);
  // A3, B2, C1, D2
  EXPECT_EQ(this->index_float_nulls->cend(), this->index_float_nulls->cbegin() + 0u);
  // A3, B2, C2, D2
  EXPECT_EQ(this->index_float_mixed->cend(), this->index_float_mixed->cbegin() + 4u);
  // A3, B2, D2, D1
  EXPECT_EQ(this->index_float_empty->cend(), this->index_float_empty->cbegin() + 0u);

  // A4, B1, C2, D2
  EXPECT_EQ(this->index_double_no_nulls->cend(), this->index_double_no_nulls->cbegin() + 10u);
  // A4, B2, C1, D2
  EXPECT_EQ(this->index_double_nulls->cend(), this->index_double_nulls->cbegin() + 0u);
  // A4, B2, C2, D2
  EXPECT_EQ(this->index_double_mixed->cend(), this->index_double_mixed->cbegin() + 4u);
  // A4, B2, D2, D1
  EXPECT_EQ(this->index_double_empty->cend(), this->index_double_empty->cbegin() + 0u);

  // A5, B1, C2, D2
  EXPECT_EQ(this->index_string_no_nulls->cend(), this->index_string_no_nulls->cbegin() + 6u);
  // A5, B2, C1, D2
  EXPECT_EQ(this->index_string_nulls->cend(), this->index_string_nulls->cbegin() + 0u);
  // A5, B2, C2, D2
  EXPECT_EQ(this->index_string_mixed->cend(), this->index_string_mixed->cbegin() + 4u);
  // A5, B2, D2, D1
  EXPECT_EQ(this->index_string_empty->cend(), this->index_string_empty->cbegin() + 0u);
}

TYPED_TEST(SingleSegmentIndexTest, NullCBeginCEndTest) {
  // A1, B1, C2, D2
  EXPECT_EQ(this->index_int_no_nulls->null_cend(), this->index_int_no_nulls->null_cbegin() + 0u);
  // A1, B2, C1, D2
  EXPECT_EQ(this->index_int_nulls->null_cend(), this->index_int_nulls->null_cbegin() + 3u);
  // A1, B2, C2, D2
  EXPECT_EQ(this->index_int_mixed->null_cend(), this->index_int_mixed->null_cbegin() + 4u);
  // A1, B2, D2, D1
  EXPECT_EQ(this->index_int_empty->null_cend(), this->index_int_empty->null_cbegin() + 0u);

  // A2, B1, C2, D2
  EXPECT_EQ(this->index_long_no_nulls->null_cend(), this->index_long_no_nulls->null_cbegin() + 0u);
  // A2, B2, C1, D2
  EXPECT_EQ(this->index_long_nulls->null_cend(), this->index_long_nulls->null_cbegin() + 3u);
  // A2, B2, C2, D2
  EXPECT_EQ(this->index_long_mixed->null_cend(), this->index_long_mixed->null_cbegin() + 4u);
  // A2, B2, D2, D1
  EXPECT_EQ(this->index_long_empty->null_cend(), this->index_long_empty->null_cbegin() + 0u);

  // A3, B1, C2, D2
  EXPECT_EQ(this->index_float_no_nulls->null_cend(), this->index_float_no_nulls->null_cbegin() + 0u);
  // A3, B2, C1, D2
  EXPECT_EQ(this->index_float_nulls->null_cend(), this->index_float_nulls->null_cbegin() + 3u);
  // A3, B2, C2, D2
  EXPECT_EQ(this->index_float_mixed->null_cend(), this->index_float_mixed->null_cbegin() + 4u);
  // A3, B2, D2, D1
  EXPECT_EQ(this->index_float_empty->null_cend(), this->index_float_empty->null_cbegin() + 0u);

  // A4, B1, C2, D2
  EXPECT_EQ(this->index_double_no_nulls->null_cend(), this->index_double_no_nulls->null_cbegin() + 0u);
  // A4, B2, C1, D2
  EXPECT_EQ(this->index_double_nulls->null_cend(), this->index_double_nulls->null_cbegin() + 3u);
  // A4, B2, C2, D2
  EXPECT_EQ(this->index_double_mixed->null_cend(), this->index_double_mixed->null_cbegin() + 4u);
  // A4, B2, D2, D1
  EXPECT_EQ(this->index_double_empty->null_cend(), this->index_double_empty->null_cbegin() + 0u);

  // A5, B1, C2, D2
  EXPECT_EQ(this->index_string_no_nulls->null_cend(), this->index_string_no_nulls->null_cbegin() + 0u);
  // A5, B2, C1, D2
  EXPECT_EQ(this->index_string_nulls->null_cend(), this->index_string_nulls->null_cbegin() + 3u);
  // A5, B2, C2, D2
  EXPECT_EQ(this->index_string_mixed->null_cend(), this->index_string_mixed->null_cbegin() + 4u);
  // A5, B2, D2, D1
  EXPECT_EQ(this->index_string_empty->null_cend(), this->index_string_empty->null_cbegin() + 0u);
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

TYPED_TEST(SingleSegmentIndexTest, CreateZeroSegmentIndexTest) {
  EXPECT_THROW(std::make_shared<TypeParam>(std::vector<std::shared_ptr<const BaseSegment>>({})), std::logic_error);
}

TYPED_TEST(SingleSegmentIndexTest, FullRangeNoNulls) {
  // int
  auto begin_int = this->index_int_no_nulls->cbegin();
  auto end_int = this->index_int_no_nulls->cend();
  auto actual_values_int = this->result_as_vector(this->dict_segment_int_no_nulls, begin_int, end_int);
  auto expected_values_int = std::vector<AllTypeVariant>{0, 1, 2, 3, 4, 4, 4, 7, 8, 9};
  EXPECT_EQ(expected_values_int, actual_values_int);

  auto actual_null_positions_int =
      std::vector<ChunkOffset>(this->index_int_no_nulls->null_cbegin(), this->index_int_no_nulls->null_cend());
  auto expected_null_positions = std::vector<ChunkOffset>{};
  EXPECT_EQ(expected_null_positions, actual_null_positions_int);

  // string
  auto begin_str = this->index_string_no_nulls->cbegin();
  auto end_str = this->index_string_no_nulls->cend();
  auto actual_values_str = this->result_as_vector(this->dict_segment_string_no_nulls, begin_str, end_str);
  auto expected_values_str = std::vector<AllTypeVariant>{"bar", "foo", "foo", "hello", "test", "world"};
  EXPECT_EQ(expected_values_str, actual_values_str);

  auto actual_null_positions_string =
      std::vector<ChunkOffset>(this->index_string_no_nulls->null_cbegin(), this->index_string_no_nulls->null_cend());
  EXPECT_EQ(expected_null_positions, actual_null_positions_string);
}

TYPED_TEST(SingleSegmentIndexTest, FullRangeNulls) {
  // int
  auto begin_int = this->index_int_nulls->cbegin();
  auto end_int = this->index_int_nulls->cend();
  auto actual_values_int = this->result_as_vector(this->dict_segment_int_nulls, begin_int, end_int);
  auto expected_values = std::vector<AllTypeVariant>{};
  EXPECT_EQ(expected_values, actual_values_int);

  auto actual_null_positions_int =
      std::vector<ChunkOffset>(this->index_int_nulls->null_cbegin(), this->index_int_nulls->null_cend());
  auto expected_null_positions = std::vector<ChunkOffset>{0u, 1u, 2u};
  EXPECT_EQ(expected_null_positions, actual_null_positions_int);

  // string
  auto begin_str = this->index_string_nulls->cbegin();
  auto end_str = this->index_string_nulls->cend();
  auto actual_values_str = this->result_as_vector(this->dict_segment_string_nulls, begin_str, end_str);
  EXPECT_EQ(expected_values, actual_values_str);

  auto actual_null_positions_string =
      std::vector<ChunkOffset>(this->index_string_nulls->null_cbegin(), this->index_string_nulls->null_cend());
  EXPECT_EQ(expected_null_positions, actual_null_positions_string);
}

TYPED_TEST(SingleSegmentIndexTest, FullRangeMixed) {
  // int
  auto begin_int = this->index_int_mixed->cbegin();
  auto end_int = this->index_int_mixed->cend();
  auto actual_values_int = this->result_as_vector(this->dict_segment_int_mixed, begin_int, end_int);
  auto expected_values_int = std::vector<AllTypeVariant>{0, 3, 3, 4};
  EXPECT_EQ(expected_values_int, actual_values_int);

  auto actual_null_positions_int =
      std::vector<ChunkOffset>(this->index_int_mixed->null_cbegin(), this->index_int_mixed->null_cend());
  auto expected_null_positions = std::vector<ChunkOffset>{0u, 2u, 4u, 7u};
  EXPECT_EQ(expected_null_positions, actual_null_positions_int);

  // string
  auto begin_str = this->index_string_mixed->cbegin();
  auto end_str = this->index_string_mixed->cend();
  auto actual_values_str = this->result_as_vector(this->dict_segment_string_mixed, begin_str, end_str);
  auto expected_values_str = std::vector<AllTypeVariant>{"alpha", "hello", "hello", "test"};
  EXPECT_EQ(expected_values_str, actual_values_str);

  auto actual_null_positions_string =
      std::vector<ChunkOffset>(this->index_string_mixed->null_cbegin(), this->index_string_mixed->null_cend());
  EXPECT_EQ(expected_null_positions, actual_null_positions_string);
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
  auto actual_values = this->result_as_vector(this->dict_segment_int_no_nulls, begin, end);

  auto expected_positions = std::set<std::size_t>{0, 4, 7};
  auto expected_values = std::vector<AllTypeVariant>{1, 2, 3};

  EXPECT_EQ(expected_positions, result_positions);
  EXPECT_EQ(expected_values, actual_values);
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
  if (!HYRISE_DEBUG || std::is_same_v<TypeParam, BTreeIndex>) GTEST_SKIP();
  auto vs_int = std::make_shared<ValueSegment<int>>();
  vs_int->append(4);

  EXPECT_THROW(std::make_shared<TypeParam>(std::vector<std::shared_ptr<const BaseSegment>>({vs_int})),
               std::logic_error);
}

}  // namespace opossum
