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
    {1, 7, 9,  12, 14, 18, 20, 24, 27},
    {3, 8, 11, 12, 16, 18, 20, 25, 27},
    {1, 2, 3,  4,  5,  6,  7,  8,  9},
    {1, 1, 3,  1,  2,  1,  1,  2,  1}
  };

  const auto histogram_b = GenericHistogram<int32_t>{
    {0,  8, 14, 20, 23, 28},
    {4, 12, 16, 21, 27, 29},
    {10,15, 12, 13, 20, 15},
    { 3, 3 , 2,  1,  2,  2}
  };
  // clang-format on

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  std::cout << merged_histogram->description(true) << std::endl;

  ASSERT_EQ(merged_histogram->bin_count(), 16u);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(0, 0, 2, 1));
  EXPECT_EQ(merged_histogram->bin(BinID{1}), histogram_bin(1, 3, 7, 7));
  EXPECT_EQ(merged_histogram->bin(BinID{2}), histogram_bin(4, 4, 2, 2));
  EXPECT_EQ(merged_histogram->bin(BinID{3}), histogram_bin(7, 7, 1, 1));
  EXPECT_EQ(merged_histogram->bin(BinID{4}), histogram_bin(8, 8, 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{5}), histogram_bin(9, 11, 12, 12));
  EXPECT_EQ(merged_histogram->bin(BinID{6}), histogram_bin(12, 12, 7, 7));
  EXPECT_EQ(merged_histogram->bin(BinID{7}), histogram_bin(14, 16, 17, 17));
  EXPECT_EQ(merged_histogram->bin(BinID{8}), histogram_bin(18, 18, 6, 6));
  EXPECT_EQ(merged_histogram->bin(BinID{9}), histogram_bin(20, 20, 14, 14));
  EXPECT_EQ(merged_histogram->bin(BinID{10}), histogram_bin(21, 21, 7, 7));
  EXPECT_EQ(merged_histogram->bin(BinID{11}), histogram_bin(23, 23, 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{12}), histogram_bin(24, 25, 16, 16));
  EXPECT_EQ(merged_histogram->bin(BinID{13}), histogram_bin(26, 26, 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{14}), histogram_bin(27, 27, 13, 13));
  EXPECT_EQ(merged_histogram->bin(BinID{15}), histogram_bin(28, 29, 15, 15));
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
    {2.0f, 5.0f},
    {3.5f, 10.0f},
    {4,    41},
    {4,    41}
  };

  const auto histogram_b = GenericHistogram<float>{
    {3.5f, 5.5f,                 next_value(6.0f)},
    {4.0f, previous_value(6.0f), 7.0f},
    {5,    25,                   15},
    {5,    25,                   15}
  };
  // clang-format on

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  ASSERT_EQ(merged_histogram->bin_count(), 6u);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(2.0f, previous_value(3.5f), 4, 4));
  EXPECT_EQ(merged_histogram->bin(BinID{1}), histogram_bin(next_value(3.5f), 4.0f, 5, 5));
  EXPECT_EQ(merged_histogram->bin(BinID{2}), histogram_bin(5.0f, previous_value(5.5f), 5, 5));
  EXPECT_EQ(merged_histogram->bin(BinID{3}), histogram_bin(5.5f, previous_value(6.0f), 30, 30));
  EXPECT_EQ(merged_histogram->bin(BinID{4}), histogram_bin(next_value(6.0f), 7.0f, 24, 24));
  EXPECT_EQ(merged_histogram->bin(BinID{5}), histogram_bin(next_value(7.0f), 10.0f, 25, 25));
}

TEST_F(HistogramUtilsTest, ReduceHistogram) {
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
}

}  // namespace opossum
