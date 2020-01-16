#include "base_test.hpp"

#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/counting_quotient_filter.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

namespace opossum {

class AttributeStatisticsTest : public BaseTest {};

TEST_F(AttributeStatisticsTest, SetStatisticsObject) {
  AttributeStatistics<int32_t> attribute_statistics;

  EXPECT_EQ(attribute_statistics.histogram, nullptr);
  EXPECT_EQ(attribute_statistics.min_max_filter, nullptr);
  EXPECT_EQ(attribute_statistics.range_filter, nullptr);
  EXPECT_EQ(attribute_statistics.null_value_ratio, nullptr);
  EXPECT_EQ(attribute_statistics.counting_quotient_filter, nullptr);

  attribute_statistics.set_statistics_object(GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20));
  EXPECT_NE(attribute_statistics.histogram, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<MinMaxFilter<int32_t>>(0, 100));
  EXPECT_NE(attribute_statistics.min_max_filter, nullptr);

  attribute_statistics.set_statistics_object(
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)}));
  EXPECT_NE(attribute_statistics.range_filter, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u));
  EXPECT_NE(attribute_statistics.counting_quotient_filter, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<NullValueRatioStatistics>(0.2f));
  EXPECT_NE(attribute_statistics.null_value_ratio, nullptr);
}

TEST_F(AttributeStatisticsTest, Scaled) {
  AttributeStatistics<int32_t> attribute_statistics;
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.counting_quotient_filter = std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u);
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);

  const auto scaled_attribute_statistics =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(attribute_statistics.scaled(0.5f));
  ASSERT_NE(scaled_attribute_statistics, nullptr);
  EXPECT_NE(scaled_attribute_statistics->histogram, nullptr);
  EXPECT_NE(scaled_attribute_statistics->min_max_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->range_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->null_value_ratio, nullptr);
  // Cannot scale CQF
  EXPECT_EQ(scaled_attribute_statistics->counting_quotient_filter, nullptr);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->min, 0);
  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->max, 100);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).first, 0);
  EXPECT_FLOAT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(attribute_statistics.null_value_ratio->ratio, 0.2f);
}

TEST_F(AttributeStatisticsTest, Sliced) {
  AttributeStatistics<int32_t> attribute_statistics;
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(1, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.counting_quotient_filter = std::make_shared<CountingQuotientFilter<int32_t>>(2u, 2u);
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);

  const auto scaled_attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(
      attribute_statistics.sliced(PredicateCondition::GreaterThanEquals, 51));
  ASSERT_NE(scaled_attribute_statistics, nullptr);
  EXPECT_NE(scaled_attribute_statistics->histogram, nullptr);
  EXPECT_NE(scaled_attribute_statistics->min_max_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->range_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->null_value_ratio, nullptr);
  // Cannot sliced CQF
  EXPECT_EQ(scaled_attribute_statistics->counting_quotient_filter, nullptr);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->min, 51);
  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->max, 100);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).first, 51);
  EXPECT_FLOAT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(attribute_statistics.null_value_ratio->ratio, 0.2f);
}

}  // namespace opossum
