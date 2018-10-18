#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"

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

}  // namespace opossum
