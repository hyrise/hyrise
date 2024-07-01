#include "base_test.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/statistics_objects/distinct_value_count.hpp"
#include "statistics/statistics_objects/min_max_filter.hpp"
#include "statistics/statistics_objects/null_value_ratio_statistics.hpp"
#include "statistics/statistics_objects/range_filter.hpp"

namespace hyrise {

class AttributeStatisticsTest : public BaseTest {};

TEST_F(AttributeStatisticsTest, SetStatisticsObject) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};

  EXPECT_FALSE(attribute_statistics.histogram);
  EXPECT_FALSE(attribute_statistics.min_max_filter);
  EXPECT_FALSE(attribute_statistics.range_filter);
  EXPECT_FALSE(attribute_statistics.null_value_ratio);
  EXPECT_FALSE(attribute_statistics.distinct_value_count);

  attribute_statistics.set_statistics_object(GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20));
  EXPECT_TRUE(attribute_statistics.histogram);

  attribute_statistics.set_statistics_object(std::make_shared<MinMaxFilter<int32_t>>(0, 100));
  EXPECT_TRUE(attribute_statistics.min_max_filter);

  attribute_statistics.set_statistics_object(
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)}));
  EXPECT_TRUE(attribute_statistics.range_filter);

  attribute_statistics.set_statistics_object(std::make_shared<NullValueRatioStatistics>(0.2f));
  EXPECT_TRUE(attribute_statistics.null_value_ratio);

  attribute_statistics.set_statistics_object(std::make_shared<DistinctValueCount>(12));
  EXPECT_TRUE(attribute_statistics.distinct_value_count);
}

TEST_F(AttributeStatisticsTest, Scaled) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(12);

  const auto scaled_attribute_statistics =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(attribute_statistics.scaled(0.5f));
  ASSERT_TRUE(scaled_attribute_statistics);
  EXPECT_TRUE(scaled_attribute_statistics->histogram);
  EXPECT_TRUE(scaled_attribute_statistics->min_max_filter);
  EXPECT_TRUE(scaled_attribute_statistics->range_filter);
  EXPECT_TRUE(scaled_attribute_statistics->null_value_ratio);
  EXPECT_TRUE(scaled_attribute_statistics->distinct_value_count);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->min, 0);
  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->max, 100);

  EXPECT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).first, 0);
  EXPECT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->null_value_ratio->ratio, 0.2f);
  EXPECT_EQ(scaled_attribute_statistics->distinct_value_count->count, 12);
}

// Scaling with a selectivity of 1.0 does not generate a new object.
TEST_F(AttributeStatisticsTest, ScaledWithIdentity) {
  const auto attribute_statistics = std::make_shared<AttributeStatistics<int32_t>>();
  const auto histogram = GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20);
  const auto min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  const auto range_filter = std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  const auto null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  const auto distinct_value_count = std::make_shared<DistinctValueCount>(12);

  attribute_statistics->histogram = histogram;
  attribute_statistics->min_max_filter = min_max_filter;
  attribute_statistics->range_filter = range_filter;
  attribute_statistics->null_value_ratio = null_value_ratio;
  attribute_statistics->distinct_value_count = distinct_value_count;

  const auto scaled_attribute_statistics =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(attribute_statistics->scaled(1.0f));
  ASSERT_TRUE(scaled_attribute_statistics);
  EXPECT_EQ(scaled_attribute_statistics, attribute_statistics);
  EXPECT_EQ(scaled_attribute_statistics->histogram, histogram);
  EXPECT_EQ(scaled_attribute_statistics->min_max_filter, min_max_filter);
  EXPECT_EQ(scaled_attribute_statistics->range_filter, range_filter);
  EXPECT_EQ(scaled_attribute_statistics->null_value_ratio, null_value_ratio);
  EXPECT_EQ(scaled_attribute_statistics->distinct_value_count, distinct_value_count);
}

TEST_F(AttributeStatisticsTest, Sliced) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(1, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(12);

  const auto sliced_attribute_statistics = std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(
      attribute_statistics.sliced(PredicateCondition::GreaterThanEquals, 51));
  ASSERT_TRUE(sliced_attribute_statistics);
  EXPECT_TRUE(sliced_attribute_statistics->histogram);
  EXPECT_TRUE(sliced_attribute_statistics->min_max_filter);
  EXPECT_TRUE(sliced_attribute_statistics->range_filter);
  EXPECT_TRUE(sliced_attribute_statistics->null_value_ratio);
  EXPECT_FALSE(sliced_attribute_statistics->distinct_value_count);

  EXPECT_FLOAT_EQ(sliced_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(sliced_attribute_statistics->min_max_filter->min, 51);
  EXPECT_EQ(sliced_attribute_statistics->min_max_filter->max, 100);

  EXPECT_EQ(sliced_attribute_statistics->range_filter->ranges.at(0).first, 51);
  EXPECT_EQ(sliced_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(sliced_attribute_statistics->null_value_ratio->ratio, 0.0f);
}

TEST_F(AttributeStatisticsTest, OutputToStream) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};

  auto stream = std::stringstream{};
  stream << attribute_statistics;

  EXPECT_EQ(stream.str(), "{\n}\n");

  stream.str("");
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(1, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector<std::pair<int32_t, int32_t>>{{0, 20}, {87, 100}});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(12);

  stream << attribute_statistics;

  const auto expected_string = std::string{
      R"({
Generic value count: 40; distinct count: 20; bin count: 1;  Bins
  [1 -> 100]: Height: 40; DistinctCount: 20

MinMaxFilter: {0, 100}
RangeFilter: { [0, 20], [87, 100] }
NullValueRatio: 0.2
DistinctValueCount: 12
}
)"};
  EXPECT_EQ(stream.str(), expected_string);
}

}  // namespace hyrise
