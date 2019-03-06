#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "statistics/statistics_objects/generic_histogram.hpp"
#include "statistics/statistics_objects/histogram_utils.hpp"

using namespace opossum::histogram;  // NOLINT

namespace opossum {

class HistogramUtilsTest : public BaseTest {};

TEST_F(HistogramUtilsTest, ValueDistributionFromColumnInt) {
  const auto table = load_table("resources/test_data/tbl/int_float6.tbl", 2);

  const auto actual_distribution = value_distribution_from_column<int32_t>(*table, ColumnID{0});
  const auto expected_distribution = std::vector<std::pair<int32_t, HistogramCountType>>{
      {12, 1},
      {123, 1},
      {12345, 2},
  };

  EXPECT_EQ(actual_distribution, expected_distribution);
}

TEST_F(HistogramUtilsTest, ValueDistributionFromColumnString) {
  // Also tests that characters out of the character range of the StringHistogramDomain are capped into the valid range
  // In this case "C" becomes "B"

  const auto table = load_table("resources/test_data/tbl/int_string2.tbl", 2);

  const auto actual_distribution =
      value_distribution_from_column<pmr_string>(*table, ColumnID{1}, StringHistogramDomain{'A', 'B', 2});
  const auto expected_distribution = std::vector<std::pair<pmr_string, HistogramCountType>>{{"A", 1}, {"B", 2}};

  EXPECT_EQ(actual_distribution, expected_distribution);
}

}  // namespace opossum
