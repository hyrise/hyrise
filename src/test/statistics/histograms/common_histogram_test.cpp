#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/equal_height_histogram.hpp"
#include "statistics/histograms/equal_width_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "statistics/empty_statistics_object.hpp"
#include "utils/load_table.hpp"

/**
 * This file tests functionality that works the same across the main histogram types (EqualDistinctCountHistogram,
 * EqualWidthHistogram, EqualHeightHistogram)
 */

namespace opossum {

template <typename T>
class AbstractHistogramIntTest : public BaseTest {
  void SetUp() override { _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl"); }

 protected:
  std::shared_ptr<Table> _int_float4;
};

using HistogramIntTypes =
    ::testing::Types<EqualDistinctCountHistogram<int32_t>, EqualWidthHistogram<int32_t>, EqualHeightHistogram<int32_t>>;
TYPED_TEST_CASE(AbstractHistogramIntTest, HistogramIntTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(AbstractHistogramIntTest, EqualsPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 11).type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_NE(histogram->estimate_cardinality(PredicateCondition::Equals, 123'456).type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 123'457).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, 1'000'000).type, EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramIntTest, LessThanPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 12).type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 13).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, 1'000'000).type, EstimateType::MatchesAll);
}

TYPED_TEST(AbstractHistogramIntTest, LessThanEqualsPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 11).type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 12).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, 1'000'000).type, EstimateType::MatchesAll);
}

TYPED_TEST(AbstractHistogramIntTest, GreaterThanEqualsPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 0).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 123'456).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 123'457).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, 1'000'000).type,
            EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramIntTest, GreaterThanPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 0).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 123'455).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 123'456).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, 1'000'000).type, EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramIntTest, BetweenPruning) {
  const auto histogram = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 11).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 12).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 123'456).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 123'457).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 0, 1'000'000).type, EstimateType::MatchesAll);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 11, 11).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 11, 12).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 11, 123'456).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 11, 123'457).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 11, 1'000'000).type, EstimateType::MatchesAll);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 12, 12).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 12, 123'456).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 12, 123'457).type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 12, 1'000'000).type, EstimateType::MatchesAll);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 123'456, 123'456).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 123'456, 123'457).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 123'456, 1'000'000).type,
            EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 123'457, 123'457).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 123'457, 1'000'000).type,
            EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 1'000'000, 0).type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Between, 1'000'000, 1'000'000).type,
            EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramIntTest, SliceWithPredicateEmptyStatistics) {
  const auto filter = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  // Check that histogram returns an EmptyStatisticsObject iff predicate will not match any data.
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::LessThan, 12)));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::LessThanEquals, 12)));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::GreaterThanEquals, 123'456)));
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::GreaterThan, 123'456)));
}

template <typename T>
class AbstractHistogramStringTest : public BaseTest {
  void SetUp() override {
    _string2 = load_table("resources/test_data/tbl/string2.tbl");
    _string3 = load_table("resources/test_data/tbl/string3.tbl");
    _int_string_like_containing2 = load_table("resources/test_data/tbl/int_string_like_containing2.tbl");
  }

 protected:
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
  std::shared_ptr<Table> _int_string_like_containing2;
};

using HistogramStringTypes = ::testing::Types<EqualDistinctCountHistogram<std::string>,
                                              EqualWidthHistogram<std::string>, EqualHeightHistogram<std::string>>;
TYPED_TEST_CASE(AbstractHistogramStringTest, HistogramStringTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(AbstractHistogramStringTest, StringConstructorTests) {
  // Histogram checks prefix length for overflow.
  EXPECT_NO_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                  StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 13u}));
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 14u}),
               std::exception);

  // Histogram rejects unsorted character ranges.
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       StringHistogramDomain{"zyxwvutsrqponmlkjihgfedcba", 13u}),
               std::exception);

  // Histogram does not support non-consecutive supported characters.
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, StringHistogramDomain{"ac", 10u}),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, DISABLED_GenerateHistogramUnsupportedCharacters) {
  // Generation should fail if we remove 'z' from the list of supported characters,
  // because it appears in the column.
  EXPECT_NO_THROW(TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                  StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u}));
  EXPECT_THROW(TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       StringHistogramDomain{"abcdefghijklmnopqrstuvwxy", 4u}),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, BinBoundsPruning) {
  auto histogram = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, "abc").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, "abcd").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, "yyzz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Equals, "yyzza").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, "abcd").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, "abcda").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, "yyzz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThan, "yyzza").type, EstimateType::MatchesAll);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, "abc").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, "abcd").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, "yyzz").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::LessThanEquals, "yyzza").type, EstimateType::MatchesAll);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, "abc").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, "abcd").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, "yyzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThanEquals, "yyzza").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "abc").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "abcd").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "yyzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "yyzza").type, EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramStringTest, LikePruning) {
  auto histogram = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%a").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%c").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "aa%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "z%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "z%foo").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "z%foo%").type, EstimateType::MatchesNone);
}

TYPED_TEST(AbstractHistogramStringTest, NotLikePruning) {
  auto histogram = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "%").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "%a").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "%c").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "a%").type, EstimateType::MatchesApproximately);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "aa%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "z%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "z%foo").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "z%foo%").type, EstimateType::MatchesAll);
}

TYPED_TEST(AbstractHistogramStringTest, NotLikePruningSpecial) {
  auto histogram =
      TypeParam::from_segment(this->_int_string_like_containing2->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 3u,
                              StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "d%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "da%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "dam%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "damp%").type, EstimateType::MatchesNone);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "dampf%").type, EstimateType::MatchesNone);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "dampfs%").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "dampfschifffahrtsgesellschaft%").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "db%").type, EstimateType::MatchesAll);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "e%").type, EstimateType::MatchesAll);
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityForStringsLongerThanPrefix) {
  auto histogram = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // The estimated cardinality depends on the type of the histogram.
  // What we want to test here is only that estimating cardinalities for strings longer than the prefix length works
  // and returns the same cardinality as the prefix-length substring of it.
  EXPECT_GT(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb").cardinality, 0.f);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb").cardinality,
            histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbba").cardinality);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb").cardinality,
            histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbbz").cardinality);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb").cardinality,
            histogram->estimate_cardinality(PredicateCondition::GreaterThan, "bbbbzzzzzzzzz").cardinality);
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityLike) {
  auto histogram = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});
  const float total_count = this->_string3->row_count();

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%").cardinality, total_count);
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::NotLike, "%").cardinality, 0.f);

  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%a").cardinality, total_count / ipow(26, 1));
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%a%").cardinality, total_count / ipow(26, 1));
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "%a%b").cardinality, total_count / ipow(26, 2));
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "foo%bar").cardinality,
            histogram->estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 3));
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "foo%bar%").cardinality,
            histogram->estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 3));

  // If the number of fixed characters is too large and the power would overflow, cap it.
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux").cardinality,
            histogram->estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 13));
  EXPECT_EQ(histogram->estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux%corge").cardinality,
            histogram->estimate_cardinality(PredicateCondition::Like, "foo%").cardinality / ipow(26, 13));
}

TYPED_TEST(AbstractHistogramStringTest, SliceWithPredicateEmptyStatistics) {
  const auto filter = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                              StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // Check that histogram returns an EmptyStatisticsObject iff predicate will not match any data.
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::LessThan, "abcd")));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::LessThanEquals, "abcd")));
  EXPECT_FALSE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::GreaterThanEquals, "yyzz")));
  EXPECT_TRUE(std::dynamic_pointer_cast<EmptyStatisticsObject>(
      filter->sliced_with_predicate(PredicateCondition::GreaterThan, "yyzz")));
}

}  // namespace opossum
