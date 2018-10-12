#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_width_histogram.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EqualWidthHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("src/test/tables/int_float4.tbl");
    _float2 = load_table("src/test/tables/float2.tbl");
    _int_int4 = load_table("src/test/tables/int_int4.tbl");
    _string3 = load_table("src/test/tables/string3.tbl");
    _string_with_prefix = load_table("src/test/tables/string_with_prefix.tbl");
    _string_like_pruning = load_table("src/test/tables/string_like_pruning.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _int_int4;
  std::shared_ptr<Table> _string3;
  std::shared_ptr<Table> _string_with_prefix;
  std::shared_ptr<Table> _string_like_pruning;
};

TEST_F(EqualWidthHistogramTest, Basic) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 6u);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).first, 5 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).first, 5 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).first, 1 / 1.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 0.f);
}

TEST_F(EqualWidthHistogramTest, UnevenBins) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 4u);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).first, 6 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).first, 6 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).first, 6 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).first, 6 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).first, 6 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).first, 1 / 1.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 2 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).first, 2 / 2.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 0.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanDistinctValuesIntEquals) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 10u);
  EXPECT_EQ(hist->bin_count(), 10u);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 100));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 123));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 10'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10'000).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 12'345));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'345).first, 4 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 12'356));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'356).first, 4 / 3.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 12'357));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'357).first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 20'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20'000).first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 50'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 50'000).first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 100'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100'000).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 123'456));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).first, 3 / 1.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 123'457));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457).first, 0.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanDistinctValuesIntLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 10u);
  EXPECT_EQ(hist->bin_count(), 10u);

  constexpr auto hist_min = 12;
  constexpr auto hist_max = 123'456;

  // First five bins are one element "wider", because the range of the column is not evenly divisible by 10.
  constexpr auto bin_width = (hist_max - hist_min + 1) / 10;
  constexpr auto bin_0_min = hist_min;
  constexpr auto bin_9_min = hist_min + 9 * bin_width + 5;

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 100));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100).first,
                  4.f * (100 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 123));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123).first,
                  4.f * (123 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 1'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000).first,
                  4.f * (1'000 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 10'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10'000).first,
                  4.f * (10'000 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 12'345));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'345).first,
                  4.f * (12'345 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 12'356));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'356).first,
                  4.f * (12'356 - bin_0_min) / (bin_width + 1));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 12'357));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'357).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 20'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 20'000).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 50'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 50'000).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 100'000));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100'000).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, bin_9_min));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, bin_9_min + 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min + 1).first,
                  4.f + 3 * (1.f / bin_width));

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 123'456));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).first,
                  4.f + 3.f * (123'456 - bin_9_min) / bin_width);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 123'457));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).first, 7.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanRepresentableValues) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 19u);
  // There must not be more bins than representable values in the column domain.
  EXPECT_EQ(hist->bin_count(), 17 - 0 + 1);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, -1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).first, 1.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).first, 3.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).first, 0.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8).first, 0.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).first, 0.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).first, 1.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 0.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 16));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 16).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).first, 1.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 0.f);
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 19));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19).first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, 0));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0).first, 0.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1).first, 1.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 2));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2).first, 4.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 3));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3).first, 5.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 4));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4).first, 5.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 5));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5).first, 6.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 6));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 6).first, 6.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 7));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 7).first, 6.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 8));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 8).first, 7.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 9));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 9).first, 7.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 10));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10).first, 7.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 11));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 11).first, 8.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 12));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).first, 8.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 13));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 13).first, 8.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 14));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 14).first, 9.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 15).first, 10.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 16));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 16).first, 10.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 17));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 17).first, 10.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 18));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 18).first, 11.f);
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, 19));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 19).first, 11.f);
}

TEST_F(EqualWidthHistogramTest, Float) {
  auto hist = EqualWidthHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 0.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 0.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f).first, 3 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f).first, 3 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f).first, 3 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 1.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.9f).first, 3 / 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2.0f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.0f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 2.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.3f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f).first, 7 / 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.4f).first, 3 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.6f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f).first, 3 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 3.9f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f).first, 3 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 4.4f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f).first, 3 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 4.5f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f).first, 3 / 2.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Equals, 6.1f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f).first, 1 / 1.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Equals, 6.2f));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f).first, 0.f);
}

TEST_F(EqualWidthHistogramTest, LessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  // The first bin's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bin_width = (123'456 - 12 + 1) / 3;

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70).first,
                  (70.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234).first,
                  (1'234.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346).first,
                  (12'346.f - 12) / (bin_width + 1) * 4);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{80'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000).first, 4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).first,
                  4.f + (123'456.f - (12 + 2 * bin_width + 1)) / bin_width * 3);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).first, 4.f + 3.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000).first, 4.f + 3.f);
}

TEST_F(EqualWidthHistogramTest, FloatLessThan) {
  auto hist = EqualWidthHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  const auto bin_width = std::nextafter(6.1f - 0.5f, std::numeric_limits<float>::infinity()) / 3;

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f).first, 0.f);

  EXPECT_FALSE(
      hist->does_not_contain(PredicateCondition::LessThan,
                             AllTypeVariant{std::nextafter(0.5f + bin_width, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(0.5f + bin_width, std::numeric_limits<float>::infinity()))
                      .first,
                  4.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f).first, (1.0f - 0.5f) / bin_width * 4);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f).first, (1.7f - 0.5f) / bin_width * 4);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f).first,
                  4.f + (2.5f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f).first,
                  4.f + (3.0f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f).first,
                  4.f + (3.3f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f).first,
                  4.f + (3.6f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f).first,
                  4.f + (3.9f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_FALSE(hist->does_not_contain(
      PredicateCondition::LessThan,
      AllTypeVariant{std::nextafter(0.5f + 2 * bin_width, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan,
                                 std::nextafter(0.5f + 2 * bin_width, std::numeric_limits<float>::infinity()))
          .first,
      4.f + 7.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{4.4f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4.4f).first,
                  4.f + 7.f + (4.4f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f).first,
                  4.f + 7.f + (5.9f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan,
                                      AllTypeVariant{std::nextafter(6.1f, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                      .first,
                  4.f + 7.f + 3.f);
}

TEST_F(EqualWidthHistogramTest, StringLessThan) {
  auto hist = EqualWidthHistogram<std::string>::from_segment(_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // "abcd"
  const auto hist_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                          1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                          3 * (ipow(26, 0)) + 1;

  // "yyzz"
  const auto hist_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                          24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                          25 * (ipow(26, 0)) + 1;

  const auto hist_width = hist_upper - hist_lower + 1;
  // Convert to float so that calculations further down are floating point divisions.
  // The division here, however, must be integral.
  // hist_width % bin_width == 1, so there is one bin storing one additional value.
  const auto bin_width = static_cast<float>(hist_width / 4u);
  const auto bin_1_width = bin_width + 1;
  const auto bin_2_width = bin_width;
  const auto bin_3_width = bin_width;
  const auto bin_4_width = bin_width;

  const auto bin_1_lower = hist_lower;
  const auto bin_2_lower = bin_1_lower + bin_1_width;
  const auto bin_3_lower = bin_2_lower + bin_2_width;
  const auto bin_4_lower = bin_3_lower + bin_3_width;

  constexpr auto bin_1_count = 4.f;
  constexpr auto bin_2_count = 5.f;
  constexpr auto bin_3_count = 4.f;
  constexpr auto bin_4_count = 3.f;
  constexpr auto total_count = bin_1_count + bin_2_count + bin_3_count + bin_4_count;

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, "aaaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa").first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").first, 0.f);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "abce"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce").first,
                  1 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").first,
                  2 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "cccc"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc").first,
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "dddd"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd").first,
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbo").first,
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp").first,
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbpa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbpa").first, bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbq").first, bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbr"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbr").first,
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ghbs"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbs").first,
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "jjjj"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj").first,
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "kkkk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk").first,
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "lzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz").first,
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnaz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnaz").first,
                  (bin_2_width - 3) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnb").first,
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnba"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnba").first,
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnbaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbaa").first, bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnbb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbb").first, bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnbc"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbc").first,
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "mnbd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbd").first,
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "pppp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp").first,
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "qqqq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq").first,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "qllo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo").first,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stal"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stal").first,
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stam"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stam").first,
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stama"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stama").first,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stan"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stan").first,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stao"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stao").first,
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "stap"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stap").first,
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "vvvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv").first,
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "xxxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx").first,
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "ycip"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip").first,
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "yyzy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy").first,
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz").first,
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "yz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz").first, total_count);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "zzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz").first, total_count);

  // Make sure that strings longer than the prefix length do not lead to errors.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThan, "zzzzzzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzzzzzz").first, total_count);
}

TEST_F(EqualWidthHistogramTest, StringLikePrefix) {
  auto hist = EqualWidthHistogram<std::string>::from_segment(_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);
  // First bin: [abcd, ghbp], so everything before is prunable.
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a").first, 0.f);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "aa%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%").first, 0.f);

  // Complexity of prefix pattern does not matter for pruning decision.
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "aa%zz%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%zz%").first, 0.f);

  // Even though "aa%" is prunable, "a%" is not!
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "a%"));
  // Since there are no values smaller than "abcd", [abcd, azzz] is the range that "a%" covers.
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "a").first);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").first);

  // No wildcard, no party.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "abcd").first,
                  hist->estimate_cardinality(PredicateCondition::Equals, "abcd").first);

  // Classic cases for prefix search.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "ab%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ab%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "ac").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "ab").first);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "c%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "d").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "c").first);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "cfoobar%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "cfoobar%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobas").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobar").first);

  // There are values matching "g%" in two bins ([abcd, ghbp], [ghbpa, mnba]), make sure both are included.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "g%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "g%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "h").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "g").first);
  EXPECT_GT(hist->estimate_cardinality(PredicateCondition::Like, "g%").first,
            hist->estimate_cardinality(PredicateCondition::LessThan, "h").first -
                hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp").first);

  // Use upper bin boundary as range limit, since there are no other values starting with y in other bins.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "y%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "z").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "y").first);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").first,
                  hist->estimate_cardinality(PredicateCondition::LessThanEquals, "yyzz").first -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "y").first);

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "z%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%").first, 0.f);
}

TEST_F(EqualWidthHistogramTest, IntBetweenPruning) {
  // One bin for each value between min and max.
  const auto hist =
      EqualWidthHistogram<int32_t>::from_segment(this->_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 19u);

  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{0}));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{1}));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{1}));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{5}));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{6}));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{10}, AllTypeVariant{12}));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Between, AllTypeVariant{14}, AllTypeVariant{17}));
}

TEST_F(EqualWidthHistogramTest, StringCommonPrefix) {
  /**
   * The strings in this table are all eight characters long, but we limit the histogram to a prefix length of four.
   * However, all of the strings in one bin start with a common prefix.
   * In this test, we make sure that the calculation strips the common prefix within bins and works as expected.
   */
  auto hist = EqualWidthHistogram<std::string>::from_segment(
      _string_with_prefix->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // We can only calculate bin edges for width-balanced histograms based on the prefix length.
  // In this case, the common prefix of all values is the prefix length, so there is only one bin.
  EXPECT_EQ(hist->bin_count(), 1u);

  const auto hist_min = 0.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                        0.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                        0.f * ipow(26, 0) + 1;
  // (repr(zzal) - repr(aaaa) + 1)
  const auto hist_width = 25.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                          25.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                          11.f * ipow(26, 0) + 1 - hist_min + 1;
  constexpr auto bin_count = 11.f;

  // Even though we cannot have multiple bins, within the single bin, we can use common prefix elimination to calculate
  // cardinalities based on bin shares.
  // Bin edges: [aaaaaaaa, aaaazzal]
  // Common prefix: 'aaaa'
  // (repr(aaam) - hist_min) / hist_width * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaaaam").first,
                  (0.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   0.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   12.f * ipow(26, 0) + 1 - hist_min) /
                      hist_width * bin_count);

  // (repr(ffpr) - hist_min) / hist_width * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaffpr").first,
                  (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   17.f * ipow(26, 0) + 1 - hist_min) /
                      hist_width * bin_count);

  // (repr(tttt) - hist_min) / hist_width * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaatttt").first,
                  (19.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 19.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * ipow(26, 0) + 1 - hist_min) /
                      hist_width * bin_count);
}

TEST_F(EqualWidthHistogramTest, StringLikePruning) {
  /**
   * This test makes sure that LIKE pruning works if the bin count of all covering bins is 0.
   * We construct a histogram with more bins than supported characters,
   * which means that for a character that no value in the column starts with, e.g., 'd', the histogram might be able
   * to prune the value "g%".
   * This is not guaranteed, because not all bins exclusively cover only values with one starting character, obviously.
   * As an example, we are not able to prune "c%" because the bin [booo, ccc] is part of "c%"
   * and also contains the value "bums".
   *
   * For more details see AbstractHistogram::can_prune.
   *
   * For reference, these are the bins:
   * [aa, annm],
   * [annn, bbaz],
   * [bbb, boon],
   * [booo, ccc],
   * [ccca, cppo],
   * [cppp, ddda],
   * [dddb, dqqp],
   * [dqqq, eeeb],
   * [eeec, errq],
   * [errr, fffc],
   * [fffd, fssr],
   * [fsss, gggd],
   * [ggge, gtts],
   * [gttt, hhhe],
   * [hhhf, huut],
   * [huuu, iiif],
   * [iiig, ivvu],
   * [ivvv, jjjg],
   * [jjjh, jwwv],
   * [jwww, kkkh],
   * [kkki, kxxw],
   * [kxxx, llli],
   * [lllj, lyyx],
   * [lyyy, mmmj],
   * [mmmk, mzzy],
   * [mzzz, nnnk],
   * [nnnl, obax],
   * [obay, oook],
   * [oool, pcbw],
   * [pcbx, pppj],
   * [pppk, qdcv],
   * [qdcw, qqqi],
   * [qqqj, redu],
   * [redv, rrrh],
   * [rrri, sfet],
   * [sfeu, sssg],
   * [sssh, tgfs],
   * [tgft, tttf],
   * [tttg, uhgr],
   * [uhgs, uuue],
   * [uuuf, vihq],
   * [vihr, vvvd],
   * [vvve, wjip],
   * [wjiq, wwwc],
   * [wwwd, xkjo],
   * [xkjp, xxxb],
   * [xxxc, ylkn],
   * [ylko, yyya],
   * [yyyb, zmlm],
   * [zmln, zzz]
   */
  auto hist = EqualWidthHistogram<std::string>::from_segment(
      _string_like_pruning->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 50u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // Not prunable, because values start with the character.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "a%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "b%"));

  // Theoretically prunable, but not with these bin edges.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "c%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "d%"));

  // Not prunable, because values start with the character.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "e%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "f%"));

  // Prunable, because all bins covering the value are 0.
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "g%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "h%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "i%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "j%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "k%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "l%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "m%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "n%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "o%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "p%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "q%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "r%"));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::Like, "s%"));

  // Not prunable, because values start with the character.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "t%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "u%"));

  // Theoretically prunable, but not with these bin edges.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "v%"));

  // Not prunable, because values start with the character.
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "w%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "x%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "y%"));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::Like, "z%"));
}

TEST_F(EqualWidthHistogramTest, SliceWithPredicate) {
  const auto hist = std::make_shared<EqualWidthHistogram<int32_t>>(
      1, 100, std::vector<HistogramCountType>{40, 30, 20, 10}, std::vector<HistogramCountType>{15, 20, 5, 10}, 0);
  auto new_hist = std::shared_ptr<GenericHistogram<int32_t>>{};

  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(hist->does_not_contain(PredicateCondition::GreaterThan, 100));

  new_hist =
      std::static_pointer_cast<GenericHistogram<int32_t>>(hist->slice_with_predicate(PredicateCondition::Equals, 15));
  // New histogram should have 15 as min and max.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 15));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 3.f);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::NotEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 23).first, 38.f / 14);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 15));
  // New bin should start at same value as before and end at 15.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 15));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 15));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 24.f / 9);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 27));
  // New bin should start at same value as before and end at 27.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 27));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 27));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).first, 40.f / 15);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 26).first, 3.f / 2);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 15));
  // New bin should start at 15 and end at same value as before.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 15));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).first, 18.f / 7);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 27));
  // New bin should start at 27 and end at same value as before.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 27));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 27));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 100));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 100));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 35).first, 29.f / 20);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::Between, 0, 17));
  // New bin should start at same value as before (because 0 is smaller than the min of the histogram) and end at 17.
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::LessThan, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::LessThanEquals, 1));
  EXPECT_FALSE(new_hist->does_not_contain(PredicateCondition::GreaterThanEquals, 17));
  EXPECT_TRUE(new_hist->does_not_contain(PredicateCondition::GreaterThan, 17));
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).first, 28.f / 11);
}

}  // namespace opossum
