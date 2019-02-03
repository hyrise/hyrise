#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EqualDistinctCountHistogramTest : public BaseTest {
  void SetUp() override {
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _string2 = load_table("resources/test_data/tbl/string2.tbl");
    _string3 = load_table("resources/test_data/tbl/string3.tbl");
    _string_with_prefix = load_table("resources/test_data/tbl/string_with_prefix.tbl");
    _string_like_pruning = load_table("resources/test_data/tbl/string_like_pruning.tbl");
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _string2;
  std::shared_ptr<Table> _string3;
  std::shared_ptr<Table> _string_with_prefix;
  std::shared_ptr<Table> _string_like_pruning;
};

TEST_F(EqualDistinctCountHistogramTest, Basic) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).cardinality, 2.5f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000).cardinality, 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, UnevenBins) {
  auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12).cardinality, 1.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456).cardinality, 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000).cardinality, 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, Float) {
  auto hist =
      EqualDistinctCountHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f).cardinality, 4 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f).cardinality, 4 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f).cardinality, 4 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f).cardinality, 4 / 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f).cardinality, 6 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f).cardinality, 6 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f).cardinality, 6 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f).cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f).cardinality, 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, String) {
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u);

  std::cout << hist->description(true) << std::endl;
  FAIL();

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa").cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ab").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ab").cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b").cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne").cardinality, 3 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "blubb").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "blubb").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bums").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bums").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "vvv").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "vvv").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "www").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "www").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx").cardinality, 4 / 3.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy").cardinality, 4 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz").cardinality, 4 / 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzz").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzz").cardinality, 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, StringPruning) {
  /**
   * 4 bins
   *  [aa, b, birne]    -> [aa, bir]
   *  [bla, bums, ttt]  -> [bla, ttt]
   *  [uuu, www, xxx]   -> [uuu, xxx]
   *  [yyy, zzz]        -> [yyy, zzz]
   */
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 3u});

  // These values are smaller than values in bin 0.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a").type, EstimateType::MatchesNone);

  // These values fall within bin 0.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aaa").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bir").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bira").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne").type, EstimateType::MatchesApproximately);

  // These values are between bin 0 and 1.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birnea").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bis").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bja").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bk").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bkz").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bl").type, EstimateType::MatchesNone);

  // These values fall within bin 1.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "c").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "mmopasdasdasd").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "s").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "t").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "tt").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt").type, EstimateType::MatchesApproximately);

  // These values are between bin 1 and 2.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttta").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "tttzzzzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uut").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uutzzzzz").type, EstimateType::MatchesNone);

  // These values fall within bin 2.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuuzzz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uv").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uvz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "v").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "w").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "wzzzzzzzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "x").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxw").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx").type, EstimateType::MatchesApproximately);

  // These values are between bin 2 and 3.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxxa").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxxzzzzzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xy").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xyzz").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "y").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyx").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyxzzzzz").type, EstimateType::MatchesNone);

  // These values fall within bin 3.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyyzzzzz").type,
            EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yz").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "z").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz").type, EstimateType::MatchesApproximately);

  // These values are greater than the upper bound of the histogram.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzza").type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzzzzz").type, EstimateType::MatchesNone);
}

TEST_F(EqualDistinctCountHistogramTest, LessThan) {
  auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70).cardinality,
                  (70.f - 12) / (123 - 12 + 1) * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234).cardinality, 2.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457).cardinality, 7.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000).type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000).cardinality, 7.f);
}

TEST_F(EqualDistinctCountHistogramTest, FloatLessThan) {
  auto hist =
      EqualDistinctCountHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f).type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f).cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f).cardinality,
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f).cardinality,
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                       std::nextafter(2.2f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(2.2f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f).cardinality, 4.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f).cardinality,
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f).cardinality,
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                       std::nextafter(3.3f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(3.3f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f + 6.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f).type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f).cardinality, 4.f + 6.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f).cardinality,
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f).type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f).cardinality,
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                       std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                .type,
            EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(6.1f, std::numeric_limits<float>::infinity()))
                      .cardinality,
                  4.f + 6.f + 4.f);
}

TEST_F(EqualDistinctCountHistogramTest, StringLessThan) {
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // "abcd"
  const auto bin_1_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           3 * (ipow(26, 0)) + 1;
  // "efgh"
  const auto bin_1_upper = 4 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 6 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           7 * (ipow(26, 0)) + 1;
  // "ijkl"
  const auto bin_2_lower = 8 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           11 * (ipow(26, 0)) + 1;
  // "mnop"
  const auto bin_2_upper = 12 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           13 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 14 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "oopp"
  const auto bin_3_lower = 14 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           14 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           15 * (ipow(26, 0)) + 1;
  // "qrst"
  const auto bin_3_upper = 16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           17 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           19 * (ipow(26, 0)) + 1;
  // "uvwx"
  const auto bin_4_lower = 20 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 22 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           23 * (ipow(26, 0)) + 1;
  // "yyzz"
  const auto bin_4_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           25 * (ipow(26, 0)) + 1;

  const auto bin_1_width = (bin_1_upper - bin_1_lower + 1.f);
  const auto bin_2_width = (bin_2_upper - bin_2_lower + 1.f);
  const auto bin_3_width = (bin_3_upper - bin_3_lower + 1.f);
  const auto bin_4_width = (bin_4_upper - bin_4_lower + 1.f);

  constexpr auto bin_1_count = 4.f;
  constexpr auto bin_2_count = 6.f;
  constexpr auto bin_3_count = 3.f;
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").type, EstimateType::MatchesApproximately);
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg").cardinality,
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh").cardinality,
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi").cardinality, bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkl").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkl").cardinality, bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkm").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkm").cardinality,
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn").cardinality,
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoo").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoo").cardinality,
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnop").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnop").cardinality,
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoq").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoq").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopp").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopp").cardinality,
                  bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopq").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopq").cardinality,
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopr").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopr").cardinality,
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

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss").cardinality,
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst").cardinality,
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwx").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwx").cardinality,
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwy").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwy").cardinality,
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwz").cardinality,
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
}

TEST_F(EqualDistinctCountHistogramTest, StringLikePrefix) {
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // First bin: [abcd, efgh], so everything before is prunable.
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

  // Use upper bin boundary as range limit, since there are no other values starting with e in other bins.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "f").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "e").cardinality);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThanEquals, "efgh").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "e").cardinality);

  // Second bin starts at ijkl, so there is a gap between efgh and ijkl.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "f%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "f%").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ii%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ii%").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "iizzzzzzzz%").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "iizzzzzzzz%").cardinality, 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, IntBetweenPruning) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 50, 60).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 123, 124).type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 124, 12'344).type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 12'344, 12'344).type, EstimateType::MatchesNone);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 12'344, 12'345).type,
            EstimateType::MatchesApproximately);
}

TEST_F(EqualDistinctCountHistogramTest, IntBetweenPruningSpecial) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 1u);

  // Make sure that pruning does not do anything stupid with one bin.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Between, 0, 1'000'000).type, EstimateType::MatchesAll);
}

TEST_F(EqualDistinctCountHistogramTest, StringCommonPrefix) {
  /**
   * The strings in this table are all eight characters long, but we limit the histogram to a prefix length of four.
   * However, all of the strings start with a common prefix ('aaaa').
   * In this test, we make sure that the calculation strips the common prefix within bins and works as expected.
   */
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string_with_prefix->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // First bin: [aaaaaaaa, aaaaaaaz].
  // Common prefix: 'aaaaaaa'
  // (repr(m) - repr(a)) / (repr(z) - repr(a) + 1) * bin_1_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaaaam").cardinality,
                  (12.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                   (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1)) /
                      (25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                       (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1) + 1) *
                      4.f);

  // Second bin: [aaaaffff, aaaaffsd].
  // Common prefix: 'aaaaff'
  // (repr(pr) - repr(ff)) / (repr(sd) - repr(ff) + 1) * bin_2_count + bin_1_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaffpr").cardinality,
                  (15.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   17.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                   (5 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                    5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1)) /
                          (18.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           3.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                           (5 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                            5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1) +
                           1) *
                          4.f +
                      4.f);

  // Second bin: [aaaappwp, aaaazzal].
  // Common prefix: 'aaaa'
  // (repr(tttt) - repr(ppwp)) / (repr(zzal) - repr(ppwp) + 1) * bin_3_count + bin_1_count + bin_2_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaatttt").cardinality,
                  (19.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 19.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * ipow(26, 0) + 1 -
                   (15.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                    15.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 22.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                    15.f * ipow(26, 0) + 1)) /
                          (25.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           25.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) +
                           1 + 11.f * ipow(26, 0) + 1 -
                           (15.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                            15.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 22.f * (ipow(26, 1) + ipow(26, 0)) +
                            1 + 15.f * ipow(26, 0) + 1) +
                           1) *
                          3.f +
                      4.f + 4.f);
}

TEST_F(EqualDistinctCountHistogramTest, StringLikeEdgePruning) {
  /**
   * This test makes sure that pruning works even if the next value after the search prefix is part of a bin.
   * In this case, we check "d%", which is handled as the range [d, e).
   * "e" is the lower edge of the bin that appears after the gap in which "d" falls.
   * A similar situation arises for "v%", but "w" should also be in a gap,
   * because the last bin starts with the next value after "w", i.e., "wa".
   * bins: [aa, bums], [e, uuu], [wa, zzz]
   * For more details see AbstractHistogram::does_not_contain.
   * We test all the other one-letter prefixes as well, because, why not.
   */
  auto hist = EqualDistinctCountHistogram<std::string>::from_segment(
      _string_like_pruning->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  // Not prunable, because values start with the character.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "b%").type, EstimateType::MatchesApproximately);

  // Prunable, because in a gap.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%").type, EstimateType::MatchesNone);

  // This is the interesting part.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "d%").type, EstimateType::MatchesNone);

  // Not prunable, because bin range is [e, uuu].
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "f%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "g%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "h%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "i%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "j%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "k%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "l%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "m%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "n%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "o%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "p%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "q%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "r%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "s%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "t%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "u%").type, EstimateType::MatchesApproximately);

  // The second more interesting test.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "v%").type, EstimateType::MatchesNone);

  // Not prunable, because bin range is [wa, zzz].
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "w%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "x%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "y%").type, EstimateType::MatchesApproximately);
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "z%").type, EstimateType::MatchesApproximately);
}

}  // namespace opossum
