#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/histograms/generic_histogram.hpp"
#include "statistics/histograms/histogram_utils.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

class HistogramUtilsTest : public BaseTest {
 protected:
  // Builder for HistogramBin which can be invoked in Macros
  template <typename T>
  HistogramBin<T> histogram_bin(const T& min, const T& max, const HistogramCountType height,
                                const HistogramCountType distinct_count) {
    return {min, max, height, distinct_count};
  }
 protected:
};

TEST_F(HistogramUtilsTest, MergeHistogramsIntA) {
  // clang-format off
  const auto histogram_a = GenericHistogram<int32_t>{
    {1, 6, 11},
    {3, 6, 18},
    {3, 4, 30},
    {2, 1,  5}
  };

  const auto histogram_b = GenericHistogram<int32_t>{
    { 1,   5,  8, 11, 16, 18, 22},
    { 3,   6,  9, 13, 16, 20, 22},
    {10,  20, 20, 40, 10, 60, 10},
    { 3,   2,  2,  2,  1,  2,  1}
  };

  const auto expected_histogram = GenericHistogram<int32_t>{
    { 1,  5,  6,  8, 11, 14, 16, 17, 18, 19, 22},
    { 3,  5,  6,  9, 13, 15, 16, 17, 18, 20, 22},
    {13, 10, 14, 20, 52,  8, 14,  4, 24, 40, 10},
    { 3,  1,  1,  2,  3,  2,  1,  1,  1,  2,  1}
  };
  // clang-format on

  const auto merged_histogram_a_b = merge_histograms(histogram_a, histogram_b);
  const auto merged_histogram_b_a = merge_histograms(histogram_b, histogram_a);

  EXPECT_EQ(expected_histogram, *merged_histogram_a_b);
  EXPECT_EQ(expected_histogram, *merged_histogram_b_a);
}

TEST_F(HistogramUtilsTest, MergeHistogramsIntB) {
  const auto histogram_a = GenericHistogram<int32_t>{{1, 2}, {1, 2}, {2, 1}, {1, 1}};
  const auto histogram_b = GenericHistogram<int32_t>{{2, 3}, {2, 3}, {1, 2}, {1, 1}};

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  ASSERT_EQ(merged_histogram->bin_count(), 3);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(1, 1, 2, 1));
  EXPECT_EQ(merged_histogram->bin(BinID{1}), histogram_bin(2, 2, 2, 1));
  EXPECT_EQ(merged_histogram->bin(BinID{2}), histogram_bin(3, 3, 2, 1));
}

TEST_F(HistogramUtilsTest, MergeHistogramsFloatCommonCase) {
  // Merge two histograms without any "edge cases" in them

  // clang-format off
  const auto histogram_a = GenericHistogram<float>{
    {0.0f, 5.5f, 14.5f},
    {3.5f, 12.0f, 16.0f},
    {14, 52, 3},
    {14, 52, 3}
  };

  const auto histogram_b = GenericHistogram<float>{
    {0.5f, 7.0f, next_value(9.5f)},
    {4.0, 9.5f, 11.5f},
    {21, 7, 20},
    {21, 7, 20}
  };
  // clang-format on

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  ASSERT_EQ(merged_histogram->bin_count(), 8u);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(0.0f, previous_value(0.5f), 2, 2));
  EXPECT_EQ(merged_histogram->bin(BinID{1}), histogram_bin(0.5f, 3.5f, 30, 30));
  EXPECT_EQ(merged_histogram->bin(BinID{2}), histogram_bin(next_value(3.5f), 4.0f, 3, 3));
  EXPECT_EQ(merged_histogram->bin(BinID{3}), histogram_bin(5.5f, previous_value(7.0f), 12, 12));
  EXPECT_EQ(merged_histogram->bin(BinID{4}), histogram_bin(7.0f, 9.5f, 27, 27));
  EXPECT_EQ(merged_histogram->bin(BinID{5}), histogram_bin(next_value(9.5f), 11.5f, 36, 36));
  EXPECT_EQ(merged_histogram->bin(BinID{6}), histogram_bin(next_value(11.5f), 12.0f, 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{7}), histogram_bin(14.5f, 16.0f, 3, 3));
}

TEST_F(HistogramUtilsTest, MergeHistogramsFloatEdgeCases) {
  /**
   * Test (potential) edge cases
   * - bin_a.max == bin_b.min
   * - bin_a.max == previous_value(bin_b.min)
   */

  // clang-format off
  const auto histogram_a = GenericHistogram<float>{
    {2.0f,  5.0f},
    {3.5f, 10.0f},
    {4,    41},
    {4,    41}
  };

  const auto histogram_b = GenericHistogram<float>{
    {3.5f,                5.5f, next_value(6.0f)},
    {4.0f, previous_value(6.0f),           7.0f},
    {5,                  25,              15},
    {5,                  25,              15}
  };
  // clang-format on

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  ASSERT_EQ(merged_histogram->bin_count(), 7u);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(2.0f, previous_value(3.5f), 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{1}), histogram_bin(next_value(3.5f), 4.0f, 5, 5));
  EXPECT_EQ(merged_histogram->bin(BinID{2}), histogram_bin(5.0f, previous_value(5.5f), 5, 5));
  EXPECT_EQ(merged_histogram->bin(BinID{3}), histogram_bin(5.5f, previous_value(6.0f), 30, 30));
  EXPECT_EQ(merged_histogram->bin(BinID{4}), histogram_bin(6.0f, 6.0f, 1, 1));
  EXPECT_EQ(merged_histogram->bin(BinID{5}), histogram_bin(next_value(6.0f), 7.0f, 24, 24));
  EXPECT_EQ(merged_histogram->bin(BinID{6}), histogram_bin(next_value(7.0f), 10.0f, 25, 25));
}

TEST_F(HistogramUtilsTest, ReduceHistogramInt) {
  // clang-format off
  const auto histogram = GenericHistogram<int32_t>{
    {0, 2, 6, 9, 13, 15},
    {1, 4, 8,11, 14, 16},
    {3, 5, 5, 4,  3,  3},
    {1, 3, 2, 2,  2,  2}
  };
  // clang-format on

  const auto reduced_histogram = reduce_histogram(histogram, 3);

  ASSERT_EQ(reduced_histogram->bin_count(), 3u);
  EXPECT_EQ(reduced_histogram->bin(BinID{0}), histogram_bin(0, 4, 8, 4));
  EXPECT_EQ(reduced_histogram->bin(BinID{1}), histogram_bin(6, 11, 9, 4));
  EXPECT_EQ(reduced_histogram->bin(BinID{2}), histogram_bin(13, 16, 6, 4));

  // Test that having less bins in the original histogram than requested for the reduced histogram works
  const auto reduced_histogram_many_bins = reduce_histogram(histogram, 60);
  EXPECT_EQ(histogram, *reduced_histogram_many_bins);
}

}  // namespace opossum
