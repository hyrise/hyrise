#include "gtest/gtest.h"

#include "statistics/column_statistics.hpp"
#include "statistics/statistics_objects/counting_quotient_filter.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"
#include "statistics/statistics_objects/single_bin_histogram.hpp"

namespace opossum {

class ColumnStatisticsTest : public ::testing::Test {};

TEST_F(ColumnStatisticsTest, SetStatisticsObject) {
  ColumnStatistics<int32_t> column_statistics;

  EXPECT_EQ(column_statistics.histogram, nullptr);
  EXPECT_EQ(column_statistics.min_max_filter, nullptr);
  EXPECT_EQ(column_statistics.range_filter, nullptr);
  EXPECT_EQ(column_statistics.null_value_ratio, nullptr);
  EXPECT_EQ(column_statistics.counting_quotient_filter, nullptr);

  column_statistics.set_statistics_object(std::make_shared<SingleBinHistogram<int32_t>>(0, 100, 40, 20));
  EXPECT_NE(column_statistics.histogram, nullptr);

  column_statistics.set_statistics_object(std::make_shared<MinMaxFilter<int32_t>>(0, 100));
  EXPECT_NE(column_statistics.min_max_filter, nullptr);

  column_statistics.set_statistics_object(
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)}));
  EXPECT_NE(column_statistics.range_filter, nullptr);

  column_statistics.set_statistics_object(std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u));
  EXPECT_NE(column_statistics.counting_quotient_filter, nullptr);

  column_statistics.set_statistics_object(std::make_shared<NullValueRatioStatistics>(0.2f));
  EXPECT_NE(column_statistics.null_value_ratio, nullptr);
}

TEST_F(ColumnStatisticsTest, Scaled) {
  ColumnStatistics<int32_t> column_statistics;
  column_statistics.histogram = std::make_shared<SingleBinHistogram<int32_t>>(0, 100, 40, 20);
  column_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  column_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  column_statistics.counting_quotient_filter = std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u);
  column_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);

  const auto scaled_column_statistics =
      std::dynamic_pointer_cast<ColumnStatistics<int32_t>>(column_statistics.scaled(0.5f));
  ASSERT_NE(scaled_column_statistics, nullptr);
  EXPECT_NE(scaled_column_statistics->histogram, nullptr);
  EXPECT_NE(scaled_column_statistics->min_max_filter, nullptr);
  EXPECT_NE(scaled_column_statistics->range_filter, nullptr);
  EXPECT_NE(scaled_column_statistics->null_value_ratio, nullptr);
  // Cannot scale CQF
  EXPECT_EQ(scaled_column_statistics->counting_quotient_filter, nullptr);

  EXPECT_FLOAT_EQ(scaled_column_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_column_statistics->min_max_filter->min, 0);
  EXPECT_EQ(scaled_column_statistics->min_max_filter->max, 100);

  EXPECT_FLOAT_EQ(scaled_column_statistics->range_filter->ranges.at(0).first, 0);
  EXPECT_FLOAT_EQ(scaled_column_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(column_statistics.null_value_ratio->ratio, 0.2f);
}

TEST_F(ColumnStatisticsTest, Sliced) {
  ColumnStatistics<int32_t> column_statistics;
  column_statistics.histogram = std::make_shared<SingleBinHistogram<int32_t>>(1, 100, 40, 20);
  column_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  column_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  column_statistics.counting_quotient_filter = std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u);
  column_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);

  const auto scaled_column_statistics = std::dynamic_pointer_cast<ColumnStatistics<int32_t>>(
      column_statistics.sliced(PredicateCondition::GreaterThanEquals, 51));
  ASSERT_NE(scaled_column_statistics, nullptr);
  EXPECT_NE(scaled_column_statistics->histogram, nullptr);
  EXPECT_NE(scaled_column_statistics->min_max_filter, nullptr);
  EXPECT_NE(scaled_column_statistics->range_filter, nullptr);
  EXPECT_NE(scaled_column_statistics->null_value_ratio, nullptr);
  // Cannot sliced CQF
  EXPECT_EQ(scaled_column_statistics->counting_quotient_filter, nullptr);

  EXPECT_FLOAT_EQ(scaled_column_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_column_statistics->min_max_filter->min, 51);
  EXPECT_EQ(scaled_column_statistics->min_max_filter->max, 100);

  EXPECT_FLOAT_EQ(scaled_column_statistics->range_filter->ranges.at(0).first, 51);
  EXPECT_FLOAT_EQ(scaled_column_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(column_statistics.null_value_ratio->ratio, 0.2f);
}

}  // namespace opossum
