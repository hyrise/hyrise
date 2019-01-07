#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"
#include "statistics/chunk_statistics/histograms/generic_histogram.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

class HistogramUtilsTest : public BaseTest {
 protected:
  uint64_t _convert_string_to_number_representation(const std::string& value) {
    return convert_string_to_number_representation(value, _supported_characters, _prefix_length);
  }

  std::string _convert_number_representation_to_string(const uint64_t value) {
    return convert_number_representation_to_string(value, _supported_characters, _prefix_length);
  }

  std::string _next_value(const std::string& value) { return next_value(value, _supported_characters, _prefix_length); }

  // Builder for HistogramBin which can be invoked in Macros
  template<typename T>
  HistogramBin<T> histogram_bin(const T& min, const T& max, const HistogramCountType height, const HistogramCountType distinct_count) {
    return {min, max, height, distinct_count};
  }

 protected:
  const std::string _supported_characters{"abcdefghijklmnopqrstuvwxyz"};
  const size_t _prefix_length{4u};
};

TEST_F(HistogramUtilsTest, NextValueString) {
  EXPECT_EQ(_next_value(""), "a");
  EXPECT_EQ(_next_value("a"), "aa");
  EXPECT_EQ(_next_value("ayz"), "ayza");
  EXPECT_EQ(_next_value("ayzz"), "az");
  EXPECT_EQ(_next_value("azzz"), "b");
  EXPECT_EQ(_next_value("z"), "za");
  EXPECT_EQ(_next_value("df"), "dfa");
  EXPECT_EQ(_next_value("abcd"), "abce");
  EXPECT_EQ(_next_value("abaz"), "abb");
  EXPECT_EQ(_next_value("abzz"), "ac");
  EXPECT_EQ(_next_value("abca"), "abcb");
  EXPECT_EQ(_next_value("abaa"), "abab");

  // Special case.
  EXPECT_EQ(_next_value("zzzz"), "zzzz");
}

TEST_F(HistogramUtilsTest, StringToNumber) {
  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);

  // 0 * (ipow(26, 3)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("a"), 1ul);

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("aa"), 2ul);

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("aaaa"), 4ul);

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 1 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("aaab"), 5ul);

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("azzz"), 18'279ul);

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("b"), 18'280ul);

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("ba"), 18'281ul);

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 7 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 9 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("bhja"), 23'447ul);

  // 2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 4 * (ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("cde"), 38'778ul);

  // 25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), 475'254ul);
}

TEST_F(HistogramUtilsTest, NumberToString) {
  EXPECT_EQ(_convert_number_representation_to_string(0ul), "");

  // 0 * (ipow(26, 3)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(1ul), "a");

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(2ul), "aa");

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(4ul), "aaaa");

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 1 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(5ul), "aaab");

  // 0 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(18'279ul), "azzz");

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(18'280ul), "b");

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(18'281ul), "ba");

  // 1 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 7 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 9 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 0 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(23'447ul), "bhja");

  // 2 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 3 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 4 * (ipow(26, 1) + ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(38'778ul), "cde");

  // 25 * (ipow(26, 3) + ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 2) + ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 1) + ipow(26, 0)) + 1 +
  // 25 * (ipow(26, 0)) + 1
  EXPECT_EQ(_convert_number_representation_to_string(475'254ul), "zzzz");
}

TEST_F(HistogramUtilsTest, CommonPrefixLength) {
  EXPECT_EQ(common_prefix_length("", ""), 0ul);
  EXPECT_EQ(common_prefix_length("a", ""), 0ul);
  EXPECT_EQ(common_prefix_length("a", "b"), 0ul);
  EXPECT_EQ(common_prefix_length("aa", "a"), 1ul);
  EXPECT_EQ(common_prefix_length("abcd", "abce"), 3ul);
}

TEST_F(HistogramUtilsTest, NumberToStringBruteForce) {
  const std::string supported_characters{"abcd"};
  constexpr size_t prefix_length{3u};
  constexpr auto max = 84ul;

  EXPECT_EQ(convert_string_to_number_representation("", supported_characters, prefix_length), 0ul);
  EXPECT_EQ(convert_string_to_number_representation("ddd", supported_characters, prefix_length), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_LT(convert_number_representation_to_string(number, supported_characters, prefix_length),
              convert_number_representation_to_string(number + 1, supported_characters, prefix_length));
  }
}

TEST_F(HistogramUtilsTest, StringToNumberBruteForce) {
  const std::string supported_characters{"abcd"};
  constexpr size_t prefix_length{3u};
  constexpr auto max = 84ul;

  EXPECT_EQ(convert_string_to_number_representation("", supported_characters, prefix_length), 0ul);
  EXPECT_EQ(convert_string_to_number_representation("ddd", supported_characters, prefix_length), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_EQ(convert_string_to_number_representation(
                  convert_number_representation_to_string(number, supported_characters, prefix_length),
                  supported_characters, prefix_length),
              number);
  }
}

TEST_F(HistogramUtilsTest, NextValueBruteForce) {
  const std::string supported_characters{"abcd"};
  constexpr size_t prefix_length{3u};
  constexpr auto max = 84ul;

  EXPECT_EQ(convert_string_to_number_representation("", supported_characters, prefix_length), 0ul);
  EXPECT_EQ(convert_string_to_number_representation("ddd", supported_characters, prefix_length), max);

  for (auto number = 1u; number <= max; number++) {
    const auto number_string = convert_number_representation_to_string(number, supported_characters, prefix_length);
    const auto next_value_of_previous_number =
        next_value(convert_number_representation_to_string(number - 1, supported_characters, prefix_length),
                   supported_characters, prefix_length);
    EXPECT_EQ(number_string, next_value_of_previous_number);
  }
}

TEST_F(HistogramUtilsTest, MergeHistogramsInt) {
  // clang-format off
  const auto histogram_a = GenericHistogram<int32_t>{
    {1, 7, 9,  12, 14, 18, 20, 24, 27},
    {3, 8, 11, 12, 16, 18, 20, 25, 27},
    {1, 2, 3,  4,  5,  6,  7,  8,  9},
    {1, 2, 3,  4,  5,  6,  7,  8,  9}
  };

  const auto histogram_b = GenericHistogram<int32_t>{
    {0,  8, 14, 20, 23, 28},
    {4, 12, 16, 21, 27, 29},
    {10,15, 12, 13, 20, 15},
    {10,15, 12, 13, 20, 15}
  };
  // clang-format on

  const auto merged_histogram = merge_histograms(histogram_a, histogram_b);

  ASSERT_EQ(merged_histogram->bin_count(), 16u);
  EXPECT_EQ(merged_histogram->bin(BinID{0}), histogram_bin(0, 0, 2, 2));
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

TEST_F(HistogramUtilsTest, MergeHistogramsFloat) {
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
