#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_height_histogram.hpp"
#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

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
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{11}));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'457}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, LessThanPruning) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{13}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, LessThanEqualsPruning) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThanEquals, AllTypeVariant{0}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThanEquals, AllTypeVariant{11}));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThanEquals, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThanEquals, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, GreaterThanEqualsPruning) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThanEquals, AllTypeVariant{0}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThanEquals, AllTypeVariant{123'456}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThanEquals, AllTypeVariant{123'457}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThanEquals, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, GreaterThanPruning) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThan, AllTypeVariant{0}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThan, AllTypeVariant{123'455}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThan, AllTypeVariant{123'456}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThan, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, BetweenPruning) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{0}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{11}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{1'000'000}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{11}, AllTypeVariant{11}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{11}, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{11}, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{11}, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{11}, AllTypeVariant{1'000'000}));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12}, AllTypeVariant{12}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12}, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12}, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12}, AllTypeVariant{1'000'000}));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123'456}, AllTypeVariant{123'456}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123'456}, AllTypeVariant{123'457}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123'456}, AllTypeVariant{1'000'000}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123'457}, AllTypeVariant{123'457}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123'457}, AllTypeVariant{1'000'000}));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{1'000'000}, AllTypeVariant{0}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{1'000'000}, AllTypeVariant{1'000'000}));
}

TYPED_TEST(AbstractHistogramIntTest, CardinalityEstimationOutOfBounds) {
  const auto hist = TypeParam::from_segment(this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);
  const auto total_count = this->_int_float4->row_count();

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::NotEquals, 11), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::NotEquals, 123'457), total_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), total_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThanEquals, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThanEquals, 123'456), total_count);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 12), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 123'457), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, 11), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, 123'456), 0.f);

  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 0, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 11, 11), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 12, 123'456), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 0, 1'000'000), total_count);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 123'457, 123'457), 0.f);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 123'457, 1'000'000), 0.f);
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

using HistogramStringTypes = ::testing::Types<EqualDistinctCountHistogram<pmr_string>, EqualWidthHistogram<pmr_string>,
                                              EqualHeightHistogram<pmr_string>>;
TYPED_TEST_CASE(AbstractHistogramStringTest, HistogramStringTypes, );  // NOLINT(whitespace/parens)

TYPED_TEST(AbstractHistogramStringTest, StringConstructorTests) {
  // Histogram checks prefix length for overflow.
  EXPECT_NO_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                          "abcdefghijklmnopqrstuvwxyz", 13u));
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       "abcdefghijklmnopqrstuvwxyz", 14u),
               std::exception);

  // Histogram rejects unsorted character ranges.
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       "zyxwvutsrqponmlkjihgfedcba", 13u),
               std::exception);

  // Histogram does not support non-consecutive supported characters.
  EXPECT_THROW(TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, "ac", 10u),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, GenerateHistogramUnsupportedCharacters) {
  // Generation should fail if we remove 'z' from the list of supported characters,
  // because it appears in the column.
  EXPECT_NO_THROW(TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                          "abcdefghijklmnopqrstuvwxyz", 4u));
  EXPECT_THROW(TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                       "abcdefghijklmnopqrstuvwxy", 4u),
               std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityUnsupportedCharacters) {
  auto hist = TypeParam::from_segment(this->_string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);

  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abcd"));

  // Allow wildcards iff predicate is (NOT) LIKE.
  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::Like, "abc_"));
  EXPECT_NO_THROW(hist->estimate_cardinality(PredicateCondition::NotLike, "abc%"));
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abc%"), std::exception);

  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "abc1"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "aBcd"), std::exception);
  EXPECT_THROW(hist->estimate_cardinality(PredicateCondition::Equals, "@abc"), std::exception);
}

TYPED_TEST(AbstractHistogramStringTest, BinEdgePruning) {
  auto hist = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "abc"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "abcd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "yyzza"));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "abcd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcda"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzza"));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThanEquals, "abc"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThanEquals, "abcd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThanEquals, "yyzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThanEquals, "yyzza"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThanEquals, "abc"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThanEquals, "abcd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThanEquals, "yyzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThanEquals, "yyzza"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThan, "abc"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::GreaterThan, "abcd"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThan, "yyzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::GreaterThan, "yyzza"));
}

TYPED_TEST(AbstractHistogramStringTest, LikePruning) {
  auto hist = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "%a"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "%c"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "a%"));

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "aa%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "z%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "z%foo"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "z%foo%"));
}

TYPED_TEST(AbstractHistogramStringTest, NotLikePruning) {
  auto hist = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "%"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "%a"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "%c"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "a%"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "aa%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "z%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "z%foo"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "z%foo%"));
}

TYPED_TEST(AbstractHistogramStringTest, NotLikePruningSpecial) {
  auto hist =
      TypeParam::from_segment(this->_int_string_like_containing2->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 3u,
                              "abcdefghijklmnopqrstuvwxyz", 4u);
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "d%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "da%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "dam%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "damp%"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::NotLike, "dampf%"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "dampfs%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "dampfschifffahrtsgesellschaft%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "db%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::NotLike, "e%"));
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityForStringsLongerThanPrefix) {
  auto hist = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);

  // The estimated cardinality depends on the type of the histogram.
  // What we want to test here is only that estimating cardinalities for strings longer than the prefix length works
  // and returns the same cardinality as the prefix-length substring of it.
  EXPECT_GT(hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb"), 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb"),
            hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbba"));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb"),
            hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbbz"));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbb"),
            hist->estimate_cardinality(PredicateCondition::GreaterThan, "bbbbzzzzzzzzz"));
}

TYPED_TEST(AbstractHistogramStringTest, EstimateCardinalityLike) {
  auto hist = TypeParam::from_segment(this->_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u,
                                      "abcdefghijklmnopqrstuvwxyz", 4u);
  const float total_count = this->_string3->row_count();

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "%"), total_count);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::NotLike, "%"), 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "%a"), total_count / ipow(26, 1));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "%a%"), total_count / ipow(26, 1));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "%a%b"), total_count / ipow(26, 2));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "foo%bar"),
            hist->estimate_cardinality(PredicateCondition::Like, "foo%") / ipow(26, 3));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "foo%bar%"),
            hist->estimate_cardinality(PredicateCondition::Like, "foo%") / ipow(26, 3));

  // If the number of fixed characters is too large and the power would overflow, cap it.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux"),
            hist->estimate_cardinality(PredicateCondition::Like, "foo%") / ipow(26, 13));
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "foo%bar%baz%qux%quux%corge"),
            hist->estimate_cardinality(PredicateCondition::Like, "foo%") / ipow(26, 13));
}

}  // namespace opossum
