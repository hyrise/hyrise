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
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _int_int4 = load_table("resources/test_data/tbl/int_int4.tbl");
    _string3 = load_table("resources/test_data/tbl/string3.tbl");
    _string_with_prefix = load_table("resources/test_data/tbl/string_with_prefix.tbl");
    _string_like_pruning = load_table("resources/test_data/tbl/string_like_pruning.tbl");
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 5 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 5 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 0.f);
}

TEST_F(EqualWidthHistogramTest, UnevenBins) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 4u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 6 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 6 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).cardinality, 6 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).cardinality, 6 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).cardinality, 6 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).cardinality, 2 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 0.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanDistinctValuesIntEquals) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 10u);
  EXPECT_EQ(hist->bin_count(), 10u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10'000).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10'000).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'345).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'345).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'356).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'356).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'357).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12'357).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 20'000).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 50'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 50'000).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 100'000).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).cardinality, 3 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'457).cardinality, 0.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanDistinctValuesIntLessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 10u);
  EXPECT_EQ(hist->bin_count(), 10u);

  constexpr auto hist_min = 12;
  constexpr auto hist_max = 123'456;

  //.cardinality five bins are one element "wider", because the range of the column is not evenly divisible by 10.
  constexpr auto bin_width = (hist_max - hist_min + 1) / 10;
  constexpr auto bin_0_min = hist_min;
  constexpr auto bin_9_min = hist_min + 9 * bin_width + 5;

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100).cardinality,
                  4.f * (100 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123).cardinality,
                  4.f * (123 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000).cardinality,
                  4.f * (1'000 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10'000).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10'000).cardinality,
                  4.f * (10'000 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'345).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'345).cardinality,
                  4.f * (12'345 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'356).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'356).cardinality,
                  4.f * (12'356 - bin_0_min) / (bin_width + 1));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'357).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'357).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 20'000).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 20'000).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 50'000).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 50'000).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100'000).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 100'000).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min + 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, bin_9_min + 1).cardinality,
                  4.f + 3 * (1.f / bin_width));

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).cardinality,
                  4.f + 3.f * (123'456 - bin_9_min) / bin_width);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).cardinality, 7.f);
}

TEST_F(EqualWidthHistogramTest, MoreBinsThanRepresentableValues) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{1}), 19u);
  // There must not be more bins than representable values in the column domain.
  EXPECT_EQ(hist->bin_count(), 17 - 0 + 1);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, -1).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1).cardinality, 3.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 5).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 7).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 8).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 9).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 11).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 13).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 14).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 16).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 16).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 17).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 19).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0).cardinality, 0.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1).cardinality, 1.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2).cardinality, 4.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3).cardinality, 5.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4).cardinality, 5.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5).cardinality, 6.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 6).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 6).cardinality, 6.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 7).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 7).cardinality, 6.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 8).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 8).cardinality, 7.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 9).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 9).cardinality, 7.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 10).cardinality, 7.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 11).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 11).cardinality, 8.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).cardinality, 8.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 13).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 13).cardinality, 8.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 14).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 14).cardinality, 9.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 15).cardinality, 10.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 16).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 16).cardinality, 10.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 17).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 17).cardinality, 10.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 18).type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 18).cardinality, 11.f);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 19).type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 19).cardinality, 11.f);
}

TEST_F(EqualWidthHistogramTest, Float) {
  auto hist = EqualWidthHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f).cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f).cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f).cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.9f).cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.0f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.0f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.1f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.2f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f).cardinality, 7 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.4f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.4f).cardinality, 3 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f).cardinality, 3 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f).cardinality, 3 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.4f).cardinality, 3 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 4.5f).cardinality, 3 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f).cardinality, 1 / 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f).cardinality, 0.f);
}

TEST_F(EqualWidthHistogramTest, LessThan) {
  auto hist =
      EqualWidthHistogram<int32_t>::from_segment(_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  // The.cardinality bin's range is one value wider (because (123'456 - 12 + 1) % 3 = 1).
  const auto bin_width = (123'456 - 12 + 1) / 3;

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{12}).type,
            EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{70}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70).cardinality,
                  (70.f - 12) / (bin_width + 1) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{1'234}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234).cardinality,
                  (1'234.f - 12) / (bin_width + 1) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{12'346}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346).cardinality,
                  (12'346.f - 12) / (bin_width + 1) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{80'000}).type,
            EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 80'000).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{123'456}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).cardinality,
                  4.f + (123'456.f - (12 + 2 * bin_width + 1)) / bin_width * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{123'457}).type,
            EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).cardinality, 4.f + 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{1'000'000}).type,
            EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000).cardinality, 4.f + 3.f);
}

TEST_F(EqualWidthHistogramTest, FloatLessThan) {
  auto hist = EqualWidthHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  const auto bin_width = std::nextafter(6.1f - 0.5f, std::numeric_limits<float>::infinity()) / 3;

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{0.5f}).type,
            EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(
                    PredicateCondition::LessThan,
                    AllTypeVariant{std::nextafter(0.5f + bin_width, std::numeric_limits<float>::infinity())})
                .type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(0.5f + bin_width, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{1.0f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f).cardinality,
                  (1.0f - 0.5f) / bin_width * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{1.7f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f).cardinality,
                  (1.7f - 0.5f) / bin_width * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{2.5f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f).cardinality,
                  4.f + (2.5f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{3.0f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f).cardinality,
                  4.f + (3.0f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{3.3f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f).cardinality,
                  4.f + (3.3f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{3.6f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f).cardinality,
                  4.f + (3.6f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{3.9f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f).cardinality,
                  4.f + (3.9f - (0.5f + bin_width)) / bin_width * 7);

  EXPECT_EQ(hist->estimate_cardinality(
                    PredicateCondition::LessThan,
                    AllTypeVariant{std::nextafter(0.5f + 2 * bin_width, std::numeric_limits<float>::infinity())})
                .type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan,
                                 std::nextafter(0.5f + 2 * bin_width, std::numeric_limits<float>::infinity()))
          .cardinality,
      4.f + 7.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{4.4f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 4.4f).cardinality,
                  4.f + 7.f + (4.4f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, AllTypeVariant{5.9f}).type,
            EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f).cardinality,
                  4.f + 7.f + (5.9f - (0.5f + 2 * bin_width)) / bin_width * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                       AllTypeVariant{std::nextafter(6.1f, std::numeric_limits<float>::infinity())})
                .type,
            EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f + 7.f + 3.f);
}

TEST_F(EqualWidthHistogramTest, FloatBinForValueLargeValues) {
  // The calculation to find out which bin a value belongs to can return a BinID that is equal to or larger than
  // the number of bins there are in the histogram.
  // See EqualWidthHistogram::_bin_for_value() for details.
  // Values are adapted from an actual error that existed previously.
  const auto min = 1'023.79f;
  const auto max = 694'486.f;
  const auto bin_count = 10'000u;
  const auto hist = EqualWidthHistogram<float>(min, max, std::vector<HistogramCountType>(bin_count, 1u),
                                               std::vector<HistogramCountType>(bin_count, 1u), 0u);
  EXPECT_NO_THROW(hist.estimate_cardinality(PredicateCondition::GreaterThanEquals, max));
}

TEST_F(EqualWidthHistogramTest, FloatBinBoundariesLargeValues) {
  // The calculation for the bin edges needs to be the same in every location in the code.
  // Previously, during creation of the histogram, we added the bin_width to the bin_minimum to
  // calculate the bin boundaries in a loop, while we divided by the bin_width to calculate the
  // boundaries for a given bin in _bin_for_value().
  // Adding the bin_width (a float for float histograms) in a loop introduces an error
  // that increases in every iteration due to floating point arithmetic.
  // In cases where there are many bins, this can result in significantly different bin boundaries,
  // such that values are put into a different bin than they were retrieved from.
  // This test checks that this is not the case.
  const auto value = 501'506.55f;
  const auto table = load_table("resources/test_data/tbl/float3.tbl");
  const auto hist =
      EqualWidthHistogram<float>::from_segment(table->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 5'000u);
  EXPECT_NE(hist->estimate_cardinality(PredicateCondition::Equals, value).cardinality, 0.0f);
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce").cardinality,
                  1 / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").cardinality,
                  2 / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "cccc").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc").cardinality,
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "dddd").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd").cardinality,
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbo").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbo").cardinality,
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp").cardinality,
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbpa").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbpa").cardinality, bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbq").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbq").cardinality, bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbr").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbr").cardinality,
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbs").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ghbs").cardinality,
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj").cardinality,
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk").cardinality,
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz").cardinality,
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnaz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnaz").cardinality,
                  (bin_2_width - 3) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnb").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnb").cardinality,
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnba").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnba").cardinality,
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbaa").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbaa").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbb").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbb").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbc").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbc").cardinality,
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbd").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnbd").cardinality,
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp").cardinality,
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stal").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stal").cardinality,
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stam").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stam").cardinality,
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stama").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stama").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stan").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stan").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stao").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stao").cardinality,
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stap").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "stap").cardinality,
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv").cardinality,
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx").cardinality,
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip").cardinality,
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy").cardinality,
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz").cardinality,
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz").cardinality, total_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz").cardinality, total_count);

  // Make sure that strings longer than the prefix length do not lead to errors.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzzzzzz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzzzzzz").cardinality, total_count);
}

TEST_F(EqualWidthHistogramTest, StringLikePrefix) {
  auto hist = EqualWidthHistogram<std::string>::from_segment(_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
                                                             4u, "abcdefghijklmnopqrstuvwxyz", 4u);
  //.cardinality bin: [abcd, ghbp], so everything before is prunable.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%").cardinality, 0.f);

  // Complexity of prefix pattern does not matter for pruning decision.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%zz%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%zz%").cardinality, 0.f);

  // Even though "aa%" is prunable, "a%" is not!
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);
  // Since there are no values smaller than "abcd", [abcd, azzz] is the range that "a%" covers.
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "a").cardinality);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").cardinality);

  // No wildcard, no party.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "abcd").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "abcd").cardinality,
                  hist->estimate_cardinality(PredicateCondition::Equals, "abcd").cardinality);

  // Classic cases for prefix search.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ab%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ab%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "ac").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "ab").cardinality);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "d").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "c").cardinality);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "cfoobar%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "cfoobar%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobas").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobar").cardinality);

  // There are values matching "g%" in two bins ([abcd, ghbp], [ghbpa, mnba]), make sure both are included.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "g%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "g%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "h").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "g").cardinality);
  EXPECT_GT(hist->estimate_cardinality(PredicateCondition::Like, "g%").cardinality,
            hist->estimate_cardinality(PredicateCondition::LessThan, "h").cardinality -
                hist->estimate_cardinality(PredicateCondition::LessThan, "ghbp").cardinality);

  // Use upper bin boundary as range limit, since there are no other values starting with y in other bins.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "z").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "y").cardinality);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThanEquals, "yyzz").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "y").cardinality);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%").cardinality, 0.f);
}

TEST_F(EqualWidthHistogramTest, IntBetweenPruning) {
  // One bin for each value between min and max.
  const auto hist =
      EqualWidthHistogram<int32_t>::from_segment(this->_int_int4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 19u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{0}).type,
            EstimateType::MatchesExactly);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{1}).type,
            EstimateType::MatchesExactly);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{1}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{5}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{1}, AllTypeVariant{6}).type,
            EstimateType::MatchesExactly);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{10}, AllTypeVariant{12}).type,
            EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, AllTypeVariant{14}, AllTypeVariant{17}).type,
            EstimateType::MatchesNone);
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
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaaaam").cardinality,
                  (0.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   0.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   12.f * ipow(26, 0) + 1 - hist_min) /
                      hist_width * bin_count);

  // (repr(ffpr) - hist_min) / hist_width * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaffpr").cardinality,
                  (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   17.f * ipow(26, 0) + 1 - hist_min) /
                      hist_width * bin_count);

  // (repr(tttt) - hist_min) / hist_width * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaatttt").cardinality,
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
   * For more details see AbstractHistogram::does_not_contain.
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
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "b%").type, EstimateType::MatchesApproximately);

  // Theoretically prunable, but not with these bin edges.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "d%").type, EstimateType::MatchesApproximately);

  // Not prunable, because values start with the character.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "f%").type, EstimateType::MatchesApproximately);

  // Prunable, because all bins covering the value are 0.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "g%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "h%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "i%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "j%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "k%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "l%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "m%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "n%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "o%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "p%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "q%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "r%").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "s%").type, EstimateType::MatchesNone);

  // Not prunable, because values start with the character.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "t%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "u%").type, EstimateType::MatchesApproximately);

  // Theoretically prunable, but not with these bin edges.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "v%").type, EstimateType::MatchesApproximately);

  // Not prunable, because values start with the character.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "w%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "x%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%").type, EstimateType::MatchesApproximately);
}

TEST_F(EqualWidthHistogramTest, SliceWithPredicate) {
  const auto hist = std::make_shared<EqualWidthHistogram<int32_t>>(
      1, 100, std::vector<HistogramCountType>{40, 30, 20, 10}, std::vector<HistogramCountType>{15, 20, 5, 10}, 0);
  auto new_hist = std::shared_ptr<GenericHistogram<int32_t>>{};

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);

  new_hist =
      std::static_pointer_cast<GenericHistogram<int32_t>>(hist->slice_with_predicate(PredicateCondition::Equals, 15));
  // New histogram should have 15 as min and max.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 15).type, EstimateType::MatchesAll);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 15).type, EstimateType::MatchesAll);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 15).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 3.f);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::NotEquals, 15));
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 23).cardinality, 38.f / 14);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 15));
  // New bin should start at same value as before and end at 15.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 15).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 15).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 24.f / 9);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::LessThanEquals, 27));
  // New bin should start at same value as before and end at 27.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 27).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 27).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 10).cardinality, 40.f / 15);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 26).cardinality, 3.f / 2);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 15));
  // New bin should start at 15 and end at same value as before.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 15).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 15).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 18).cardinality, 18.f / 7);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::GreaterThanEquals, 27));
  // New bin should start at 27 and end at same value as before.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 27).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 27).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 100).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 100).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 35).cardinality, 29.f / 20);

  new_hist = std::static_pointer_cast<GenericHistogram<int32_t>>(
      hist->slice_with_predicate(PredicateCondition::Between, 0, 17));
  // New bin should start at same value as before (because 0 is smaller than the min of the histogram) and end at 17.
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThan, 1).type, EstimateType::MatchesNone);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::LessThanEquals, 1).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThanEquals, 17).type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(new_hist->estimate_cardinality(PredicateCondition::GreaterThan, 17).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(new_hist->estimate_cardinality(PredicateCondition::Equals, 15).cardinality, 28.f / 11);
}

}  // namespace opossum
