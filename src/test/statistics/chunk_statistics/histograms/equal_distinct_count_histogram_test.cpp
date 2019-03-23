#include <limits>
#include <memory>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/equal_distinct_count_histogram.hpp"
#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
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

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 2.5f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, UnevenBins) {
  auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 12), 1.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'234), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 123'456), 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1'000'000), 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, Float) {
  auto hist =
      EqualDistinctCountHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0.4f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.4f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 0.5f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1.1f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.1f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{1.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 1.3f), 4 / 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.2f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.2f), 4 / 4.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.3f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.5f), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{2.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 2.9f), 6 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.3f), 6 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.6f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 3.9f), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{6.1f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.1f), 4 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, AllTypeVariant{6.2f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, 6.2f), 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, String) {
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "a"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "aa"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ab"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ab"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "b"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "b"), 3 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "birne"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "birne"), 3 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "biscuit"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "biscuit"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bla"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bla"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "blubb"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "blubb"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bums"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "bums"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ttt"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "ttt"), 4 / 3.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "turkey"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "turkey"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuu"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "uuu"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "vvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "vvv"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "www"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "www"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "xxx"), 4 / 3.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "yyy"), 4 / 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzz"), 4 / 2.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "zzzzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Equals, "zzzzzz"), 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, StringPruning) {
  /**
   * 4 bins
   *  [aa, b, birne]    -> [aa, bir]
   *  [bla, bums, ttt]  -> [bla, ttt]
   *  [uuu, www, xxx]   -> [uuu, xxx]
   *  [yyy, zzz]        -> [yyy, zzz]
   */
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 3u);

  // These values are smaller than values in bin 0.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, ""));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "a"));

  // These values fall within bin 0.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aa"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "aaa"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "b"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bir"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bira"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "birne"));

  // These values are between bin 0 and 1.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "birnea"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bis"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "biscuit"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bja"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bk"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bkz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "bl"));

  // These values fall within bin 1.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "bla"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "c"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "mmopasdasdasd"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "s"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "t"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "tt"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "ttt"));

  // These values are between bin 1 and 2.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "ttta"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "tttzzzzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "turkey"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "uut"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "uutzzzzz"));

  // These values fall within bin 2.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuu"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uuuzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uv"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "uvz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "v"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "w"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "wzzzzzzzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "x"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxw"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "xxx"));

  // These values are between bin 2 and 3.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xxxa"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xxxzzzzzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xy"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "xyzz"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "y"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "yyx"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "yyxzzzzz"));

  // These values fall within bin 3.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyy"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yyyzzzzz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "yz"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "z"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Equals, "zzz"));

  // These values are greater than the upper bound of the histogram.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "zzza"));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Equals, "zzzzzzzzz"));
}

TEST_F(EqualDistinctCountHistogramTest, LessThan) {
  auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      _int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{70}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 70), (70.f - 12) / (123 - 12 + 1) * 2);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'234}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'234), 2.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{12'346}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 12'346), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'456}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'456), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{123'457}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 123'457), 7.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1'000'000}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1'000'000), 7.f);
}

TEST_F(EqualDistinctCountHistogramTest, FloatLessThan) {
  auto hist =
      EqualDistinctCountHistogram<float>::from_segment(_float2->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{0.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 0.5f), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.0f),
                  (1.0f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{1.7f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 1.7f),
                  (1.7f - 0.5f) / std::nextafter(2.2f - 0.5f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(2.2f, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(2.2f, std::numeric_limits<float>::infinity())),
                  4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{2.5f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 2.5f), 4.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.0f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.0f),
                  4.f + (3.0f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.3f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.3f),
                  4.f + (3.3f - 2.5f) / std::nextafter(3.3f - 2.5f, std::numeric_limits<float>::infinity()) * 6);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(3.3f, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(3.3f, std::numeric_limits<float>::infinity())),
                  4.f + 6.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.6f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.6f), 4.f + 6.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{3.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 3.9f),
                  4.f + 6.f + (3.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, AllTypeVariant{5.9f}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, 5.9f),
                  4.f + 6.f + (5.9f - 3.6f) / std::nextafter(6.1f - 3.6f, std::numeric_limits<float>::infinity()) * 4);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan,
                               AllTypeVariant{std::nextafter(6.1f, std::numeric_limits<float>::infinity())}));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan,
                                             std::nextafter(6.1f, std::numeric_limits<float>::infinity())),
                  4.f + 6.f + 4.f);
}

TEST_F(EqualDistinctCountHistogramTest, StringLessThan) {
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u);

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

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "aaaa"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaa"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::LessThan, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"), 0.f);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abce"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abce"), 1 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "abcf"), 2 / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "abcf"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "cccc"),
      (2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 2 * (ipow(26, 1) + ipow(26, 0)) + 1 + 2 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "dddd"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "dddd"),
      (3 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 3 * (ipow(26, 1) + ipow(26, 0)) + 1 + 3 * (ipow(26, 0)) + 1 - bin_1_lower) /
          bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgg"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgg"),
                  (bin_1_width - 2) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgh"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgh"),
                  (bin_1_width - 1) / bin_1_width * bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "efgi"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "efgi"), bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkl"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkl"), bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkm"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkm"),
                  1 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ijkn"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ijkn"),
                  2 / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "jjjj"));
  EXPECT_FLOAT_EQ(
      hist->estimate_cardinality(PredicateCondition::LessThan, "jjjj"),
      (9 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) +
       1 + 9 * (ipow(26, 1) + ipow(26, 0)) + 1 + 9 * (ipow(26, 0)) + 1 - bin_2_lower) /
              bin_2_width * bin_2_count +
          bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "kkkk"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "kkkk"),
                  (10 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 10 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   10 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "lzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "lzzz"),
                  (11 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   25 * (ipow(26, 0)) + 1 - bin_2_lower) /
                          bin_2_width * bin_2_count +
                      bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnoo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoo"),
                  (bin_2_width - 2) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnop"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnop"),
                  (bin_2_width - 1) / bin_2_width * bin_2_count + bin_1_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "mnoq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "mnoq"), bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopp"), bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopq"),
                  1 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "oopr"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "oopr"),
                  2 / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "pppp"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "pppp"),
                  (15 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 15 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qqqq"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qqqq"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 16 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   16 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qllo"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qllo"),
                  (16 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   11 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 11 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   14 * (ipow(26, 0)) + 1 - bin_3_lower) /
                          bin_3_width * bin_3_count +
                      bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrss"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrss"),
                  (bin_3_width - 2) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrst"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrst"),
                  (bin_3_width - 1) / bin_3_width * bin_3_count + bin_1_count + bin_2_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "qrsu"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "qrsu"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwx"),
                  bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwy"),
                  1 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "uvwz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "uvwz"),
                  2 / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "vvvv"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "vvvv"),
                  (21 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 21 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   21 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "xxxx"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "xxxx"),
                  (23 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 23 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   23 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "ycip"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "ycip"),
                  (24 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
                   2 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 + 8 * (ipow(26, 1) + ipow(26, 0)) + 1 +
                   15 * (ipow(26, 0)) + 1 - bin_4_lower) /
                          bin_4_width * bin_4_count +
                      bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzy"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzy"),
                  (bin_4_width - 2) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yyzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yyzz"),
                  (bin_4_width - 1) / bin_4_width * bin_4_count + bin_1_count + bin_2_count + bin_3_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "yz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "yz"), total_count);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::LessThan, "zzzz"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "zzzz"), total_count);
}

TEST_F(EqualDistinctCountHistogramTest, StringLikePrefix) {
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string3->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 4u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // First bin: [abcd, efgh], so everything before is prunable.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "aa%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%"), 0.f);

  // Complexity of prefix pattern does not matter for pruning decision.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "aa%zz%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "aa%zz%"), 0.f);

  // Even though "aa%" is prunable, "a%" is not!
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "a%"));
  // Since there are no values smaller than "abcd", [abcd, azzz] is the range that "a%" covers.
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "a"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "a%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "b") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "abcd"));

  // No wildcard, no party.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "abcd"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "abcd"),
                  hist->estimate_cardinality(PredicateCondition::Equals, "abcd"));

  // Classic cases for prefix search.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "ab%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ab%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "ac") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "ab"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "c%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "c%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "d") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "c"));

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "cfoobar%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "cfoobar%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobas") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "cfoobar"));

  // Use upper bin boundary as range limit, since there are no other values starting with e in other bins.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "e%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%"),
                  hist->estimate_cardinality(PredicateCondition::LessThan, "f") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "e"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "e%"),
                  hist->estimate_cardinality(PredicateCondition::LessThanEquals, "efgh") -
                      hist->estimate_cardinality(PredicateCondition::LessThan, "e"));

  // Second bin starts at ijkl, so there is a gap between efgh and ijkl.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "f%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "f%"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "ii%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "ii%"), 0.f);

  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "iizzzzzzzz%"));
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::Like, "iizzzzzzzz%"), 0.f);
}

TEST_F(EqualDistinctCountHistogramTest, IntBetweenPruning) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 2u);

  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{50}, AllTypeVariant{60}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{123}, AllTypeVariant{124}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{124}, AllTypeVariant{12'344}));
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12'344}, AllTypeVariant{12'344}));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{12'344}, AllTypeVariant{12'345}));
}

TEST_F(EqualDistinctCountHistogramTest, IntBetweenPruningSpecial) {
  const auto hist = EqualDistinctCountHistogram<int32_t>::from_segment(
      this->_int_float4->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 1u);

  // Make sure that pruning does not do anything stupid with one bin.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Between, AllTypeVariant{0}, AllTypeVariant{1'000'000}));
}

TEST_F(EqualDistinctCountHistogramTest, StringCommonPrefix) {
  /**
   * The strings in this table are all eight characters long, but we limit the histogram to a prefix length of four.
   * However, all of the strings start with a common prefix ('aaaa').
   * In this test, we make sure that the calculation strips the common prefix within bins and works as expected.
   */
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string_with_prefix->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // First bin: [aaaaaaaa, aaaaaaaz].
  // Common prefix: 'aaaaaaa'
  // (repr(m) - repr(a)) / (repr(z) - repr(a) + 1) * bin_1_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaaaam"),
                  (12.f * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                   (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1)) /
                      (25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 -
                       (0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1) + 1) *
                      4.f);

  // Second bin: [aaaaffff, aaaaffsd].
  // Common prefix: 'aaaaff'
  // (repr(pr) - repr(ff)) / (repr(sd) - repr(ff) + 1) * bin_2_count + bin_1_count
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaaffpr"),
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
  EXPECT_FLOAT_EQ(hist->estimate_cardinality(PredicateCondition::LessThan, "aaaatttt"),
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
   * For more details see AbstractHistogram::can_prune.
   * We test all the other one-letter prefixes as well, because, why not.
   */
  auto hist = EqualDistinctCountHistogram<pmr_string>::from_segment(
      _string_like_pruning->get_chunk(ChunkID{0})->get_segment(ColumnID{0}), 3u, "abcdefghijklmnopqrstuvwxyz", 4u);

  // Not prunable, because values start with the character.
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "a%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "b%"));

  // Prunable, because in a gap.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "c%"));

  // This is the interesting part.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "d%"));

  // Not prunable, because bin range is [e, uuu].
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "e%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "f%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "g%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "h%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "i%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "j%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "k%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "l%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "m%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "n%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "o%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "p%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "q%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "r%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "s%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "t%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "u%"));

  // The second more interesting test.
  EXPECT_TRUE(hist->can_prune(PredicateCondition::Like, "v%"));

  // Not prunable, because bin range is [wa, zzz].
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "w%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "x%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "y%"));
  EXPECT_FALSE(hist->can_prune(PredicateCondition::Like, "z%"));
}

}  // namespace opossum
