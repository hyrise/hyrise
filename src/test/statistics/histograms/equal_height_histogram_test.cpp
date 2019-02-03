#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/equal_height_histogram.hpp"
#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class EqualHeightHistogramTest : public BaseTest {
 public:
  void SetUp() override {
    _int_float4 = load_table("resources/test_data/tbl/int_float4.tbl");
    _float2 = load_table("resources/test_data/tbl/float2.tbl");
    _string3 = load_table("resources/test_data/tbl/string3.tbl");
    _string_with_prefix = load_table("resources/test_data/tbl/string_with_prefix.tbl");

    int_segment = _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0});
    float_segment = _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{1});
  }

  template<typename T>
  HistogramBin<T> histogram_bin(const T& min, const T& max, const HistogramCountType height, const HistogramCountType distinct_count) {
    return HistogramBin<T>{min, max, height, distinct_count};
  }

 protected:
  std::shared_ptr<Table> _int_float4;
  std::shared_ptr<Table> _float2;
  std::shared_ptr<Table> _string3;
  std::shared_ptr<Table> _string_with_prefix;

  std::shared_ptr<BaseSegment> int_segment, float_segment;
};

TEST_F(EqualHeightHistogramTest, FromSegmentInt) {
  const auto actual_histogram = EqualHeightHistogram<int32_t>::from_segment(int_segment, 4u);

  const auto expected_height = 7.0f / 3.0f;

  ASSERT_EQ(actual_histogram->bin_count(), 3u);
  EXPECT_EQ(actual_histogram->bin(BinID{0}), histogram_bin(12, 123, expected_height, 2));
  EXPECT_EQ(actual_histogram->bin(BinID{1}), histogram_bin(124, 12345, expected_height, 1));
  EXPECT_EQ(actual_histogram->bin(BinID{2}), histogram_bin(12346, 123456, expected_height, 1));
}

TEST_F(EqualHeightHistogramTest, FromSegmentFloat) {
  const auto actual_histogram = EqualHeightHistogram<float>::from_segment(float_segment, 4u);

  const auto expected_height = 1.75f;

  ASSERT_EQ(actual_histogram->bin_count(), 4u);
  EXPECT_EQ(actual_histogram->bin(BinID{0}), histogram_bin(350.7f, 456.7f, expected_height, 2));
  EXPECT_EQ(actual_histogram->bin(BinID{1}), histogram_bin(next_value(456.7f), 458.7f, expected_height, 2));
  EXPECT_EQ(actual_histogram->bin(BinID{2}), histogram_bin(next_value(458.7f), 800.0f, expected_height, 2));
  EXPECT_EQ(actual_histogram->bin(BinID{3}), histogram_bin(next_value(800.0f), 900.0f, expected_height, 1));
}

TEST_F(EqualHeightHistogramTest, StringLessThan) {
  auto hist = EqualHeightHistogram<std::string>::from_segment(_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
                                                              4u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  std::cout << hist->description(true) << std::endl;

  // The lower bin edges are the next value after the upper edge of the previous bin.
  // The reason is that in EqualHeightHistograms the upper bin edges are taken as existing in the columns
  // (without prefix), and the lower bin edge of the next bin is simply the next string, which is the upper bin edge
  // followed by the first supported character ('a' in this case).
  // "abcd"
  const auto bin_1_lower = 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           1 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           3 * (ipow(26, 0)) + 1;
  // "efgh"
  const auto bin_1_upper = 4 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 6 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           7 * (ipow(26, 0)) + 1;
  // "efgha"
  const auto bin_2_lower = bin_1_upper + 1;
  // "kkkk"
  const auto bin_2_upper = 10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           10 * (ipow(26, 0)) + 1;
  // "kkkka"
  const auto bin_3_lower = bin_2_upper + 1;
  // "qrst"
  const auto bin_3_upper = 16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           17 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           19 * (ipow(26, 0)) + 1;
  // "qrsta"
  const auto bin_4_lower = bin_3_upper + 1;
  // "yyzz"
  const auto bin_4_upper = 24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           24 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                           25 * (ipow(26, 0)) + 1;

  const auto bin_1_width = (bin_1_upper - bin_1_lower + 1.f);
  const auto bin_2_width = (bin_2_upper - bin_2_lower + 1.f);
  const auto bin_3_width = (bin_3_upper - bin_3_lower + 1.f);
  const auto bin_4_width = (bin_4_upper - bin_4_lower + 1.f);

  constexpr auto bin_count = 4.f;
  constexpr auto total_count = 4 * bin_count;

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").type, EstimateType::MatchesNone);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd").cardinality, 0.f);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce").cardinality,
                  1 / bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf").cardinality,
                  2 / bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "cccc").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc").cardinality,
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "dddd").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd").cardinality,
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg").cardinality,
                  (bin_1_width - 2) / bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh").cardinality,
                  (bin_1_width - 1) / bin_1_width * bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgha").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgha").cardinality, bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi").cardinality, bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgj").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgj").cardinality,
                  1 / bin_2_width * bin_count + bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgk").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgk").cardinality,
                  2 / bin_2_width * bin_count + bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn").cardinality,
      (8 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 + 13 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_count +
          bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj").cardinality,
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_count +
          bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jzzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "jzzz").cardinality,
                  (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_count +
                      bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kaab").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kaab").cardinality,
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   1 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_count +
                      bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkj").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkj").cardinality,
                  (bin_2_width - 2) / bin_2_width * bin_count + bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk").cardinality,
                  (bin_2_width - 1) / bin_2_width * bin_count + bin_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkka").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkka").cardinality, bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkl").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkl").cardinality, bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkm").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkm").cardinality,
                  1 / bin_3_width * bin_count + bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkn").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkn").cardinality,
                  2 / bin_3_width * bin_count + bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "loos").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "loos").cardinality,
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 14 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   18 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "nnnn").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "nnnn").cardinality,
                  (13 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   13 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 13 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   13 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq").cardinality,
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_count +
                      bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss").cardinality,
                  (bin_3_width - 2) / bin_3_width * bin_count + bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst").cardinality,
                  (bin_3_width - 1) / bin_3_width * bin_count + bin_count * 2);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsta").type, EstimateType::MatchesExactly);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsta").cardinality, bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu").cardinality, bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsv").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsv").cardinality,
                  1 / bin_4_width * bin_count + bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsw").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsw").cardinality,
                  2 / bin_4_width * bin_count + bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "tdzr").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "tdzr").cardinality,
                  (19 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   17 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv").cardinality,
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx").cardinality,
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip").cardinality,
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_count +
                      bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy").cardinality,
                  (bin_4_width - 2) / bin_4_width * bin_count + bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz").cardinality,
                  (bin_4_width - 1) / bin_4_width * bin_count + bin_count * 3);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz").cardinality, total_count);

  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz").type, EstimateType::MatchesAll);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz").cardinality, total_count);

  FAIL();
}

TEST_F(EqualHeightHistogramTest, StringLikePrefix) {
  auto hist = EqualHeightHistogram<std::string>::from_segment(_string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}),
                                                              4u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

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

  // There are values matching "e%" in two bins ([abcd, efgh] and [efgha, kkkk]), make sure both are included.
  EXPECT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").type, EstimateType::MatchesApproximately);
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
                  hist->estimate_cardinality(PredicateCondition::LessThan, "f").cardinality -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "e").cardinality);
  EXPECT_GT(hist->estimate_cardinality(PredicateCondition::Like, "e%").cardinality,
            hist->estimate_cardinality(PredicateCondition::LessThan, "f").cardinality -
                hist->estimate_cardinality(PredicateCondition::LessThan, "efgh").cardinality);

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

TEST_F(EqualHeightHistogramTest, StringCommonPrefix) {
  /**
   * The strings in this table are all eight characters long, but we limit the histogram to a prefix length of four.
   * However, all of the strings start with a common prefix ('aaaa').
   * In this test, we make sure that the calculation strips the common prefix within bins and works as expected.
   */
  auto hist = EqualHeightHistogram<std::string>::from_segment(
      _string_with_prefix->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, StringHistogramDomain{"abcdefghijklmnopqrstuvwxyz", 4u});

  constexpr auto bin_count = 4.f;

  // First bin: [aaaaaaaa, aaaaaaaz].
  // Common prefix: 'aaaaaaa'
  // (repr(m) - repr(a)) / (repr(z) - repr(a) + 1) * bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaaaam").cardinality,
                  (12.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                   (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1)) /
                      (25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                       (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1) + 1) *
                      bin_count);

  // Second bin: [aaaaaaaza, aaaaffsd].
  // Common prefix: 'aaaa'
  // (repr(ffpr) - repr(aaaza)) / (repr(ffsd) - repr(aaaza) + 1) * bin_count + bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaffpr").cardinality,
                  (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   17.f * ipow(26, 0) + 1 -
                   (0.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                    0.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                    25.f * ipow(26, 0) + 1 + 1)) /
                          (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18.f * (ipow(26, 1) + ipow(26, 0)) +
                           1 + 3.f * ipow(26, 0) + 1 -
                           (0.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                            0.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) +
                            1 + 25.f * ipow(26, 0) + 1 + 1) +
                           1) *
                          bin_count +
                      bin_count);

  // Second bin: [aaaaffsda, aaaazzal].
  // Common prefix: 'aaaa'
  // (repr(tttt) - repr(ffsda)) / (repr(zzal) - repr(ffsda) + 1) * bin_count + bin_count + bin_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaatttt").cardinality,
                  (19.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 19.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   19.f * ipow(26, 0) + 1 -
                   (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                    5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18.f * (ipow(26, 1) + ipow(26, 0)) + 1 +
                    3.f * ipow(26, 0) + 1 + 1)) /
                          (25.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                           25.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 0.f * (ipow(26, 1) + ipow(26, 0)) +
                           1 + 11.f * ipow(26, 0) + 1 -
                           (5.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                            5.f * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 18.f * (ipow(26, 1) + ipow(26, 0)) +
                            1 + 3.f * ipow(26, 0) + 1 + 1) +
                           1) *
                          bin_count +
                      bin_count + bin_count);
}

}  // namespace opossum
