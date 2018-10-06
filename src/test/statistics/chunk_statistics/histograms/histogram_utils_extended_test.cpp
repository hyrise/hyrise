#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/chunk_statistics/histograms/histogram_utils.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

/**
 * These tests might take a few seconds and are therefore not included in the regular test suite.
 */
class HistogramUtilsExtendedTest : public BaseTest {
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

TEST_F(HistogramUtilsExtendedTest, NumberToStringBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_LT(_convert_number_representation_to_string(number), _convert_number_representation_to_string(number + 1));
  }
}

TEST_F(HistogramUtilsExtendedTest, StringToNumberBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 0u; number < max; number++) {
    EXPECT_EQ(_convert_string_to_number_representation(_convert_number_representation_to_string(number)), number);
  }
}

TEST_F(HistogramUtilsExtendedTest, NextValueBruteForce) {
  constexpr auto max = 475'254ul;

  EXPECT_EQ(_convert_string_to_number_representation(""), 0ul);
  EXPECT_EQ(_convert_string_to_number_representation("zzzz"), max);

  for (auto number = 1u; number <= max; number++) {
    const auto number_string = _convert_number_representation_to_string(number);
    const auto next_value_of_previous_number = _next_value(_convert_number_representation_to_string(number - 1));
    EXPECT_EQ(number_string, next_value_of_previous_number);
  }
}

}  // namespace opossum
