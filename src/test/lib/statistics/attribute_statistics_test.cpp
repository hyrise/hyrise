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

  EXPECT_EQ(attribute_statistics.histogram, nullptr);
  EXPECT_EQ(attribute_statistics.min_max_filter, nullptr);
  EXPECT_EQ(attribute_statistics.range_filter, nullptr);
  EXPECT_EQ(attribute_statistics.null_value_ratio, nullptr);
  EXPECT_EQ(attribute_statistics.distinct_value_count, nullptr);

  attribute_statistics.set_statistics_object(GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20));
  EXPECT_NE(attribute_statistics.histogram, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<MinMaxFilter<int32_t>>(0, 100));
  EXPECT_NE(attribute_statistics.min_max_filter, nullptr);

  attribute_statistics.set_statistics_object(
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)}));
  EXPECT_NE(attribute_statistics.range_filter, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<NullValueRatioStatistics>(0.2f));
  EXPECT_NE(attribute_statistics.null_value_ratio, nullptr);

  attribute_statistics.set_statistics_object(std::make_shared<DistinctValueCount>(123));
  EXPECT_NE(attribute_statistics.distinct_value_count, nullptr);
}

TEST_F(AttributeStatisticsTest, Scaled) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(0, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(123);

  const auto scaled_attribute_statistics =
      std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(attribute_statistics.scaled(0.5f));
  ASSERT_NE(scaled_attribute_statistics, nullptr);
  EXPECT_NE(scaled_attribute_statistics->histogram, nullptr);
  EXPECT_NE(scaled_attribute_statistics->min_max_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->range_filter, nullptr);
  EXPECT_NE(scaled_attribute_statistics->null_value_ratio, nullptr);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->min, 0);
  EXPECT_EQ(scaled_attribute_statistics->min_max_filter->max, 100);

  EXPECT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).first, 0);
  EXPECT_EQ(scaled_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(scaled_attribute_statistics->null_value_ratio->ratio, 0.2f);
  EXPECT_EQ(scaled_attribute_statistics->distinct_value_count->count, 123);
}

TEST_F(AttributeStatisticsTest, Sliced) {
  auto attribute_statistics = AttributeStatistics<int32_t>{};
  attribute_statistics.histogram = GenericHistogram<int32_t>::with_single_bin(1, 100, 40, 20);
  attribute_statistics.min_max_filter = std::make_shared<MinMaxFilter<int32_t>>(0, 100);
  attribute_statistics.range_filter =
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(123);

  const auto sliced_attribute_statistics = std::dynamic_pointer_cast<AttributeStatistics<int32_t>>(
      attribute_statistics.sliced(PredicateCondition::GreaterThanEquals, 51));
  ASSERT_NE(sliced_attribute_statistics, nullptr);
  EXPECT_NE(sliced_attribute_statistics->histogram, nullptr);
  EXPECT_NE(sliced_attribute_statistics->min_max_filter, nullptr);
  EXPECT_NE(sliced_attribute_statistics->range_filter, nullptr);
  EXPECT_NE(sliced_attribute_statistics->null_value_ratio, nullptr);

  EXPECT_FLOAT_EQ(sliced_attribute_statistics->histogram->total_count(), 20);

  EXPECT_EQ(sliced_attribute_statistics->min_max_filter->min, 51);
  EXPECT_EQ(sliced_attribute_statistics->min_max_filter->max, 100);

  EXPECT_EQ(sliced_attribute_statistics->range_filter->ranges.at(0).first, 51);
  EXPECT_EQ(sliced_attribute_statistics->range_filter->ranges.at(0).second, 100);

  EXPECT_FLOAT_EQ(sliced_attribute_statistics->null_value_ratio->ratio, 0.2f);
  EXPECT_EQ(sliced_attribute_statistics->distinct_value_count->count, 123);
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
      std::make_shared<RangeFilter<int32_t>>(std::vector{std::pair<int32_t, int32_t>(0, 100)});
  attribute_statistics.null_value_ratio = std::make_shared<NullValueRatioStatistics>(0.2f);
  attribute_statistics.distinct_value_count = std::make_shared<DistinctValueCount>(123);

  stream << attribute_statistics;

  const auto expected_string = std::string{
      R"({
Generic value count: 40; distinct count: 20; bin count: 1;  Bins
  [1 -> 100]: Height: 40; DistinctCount: 20

Has MinMaxFilter
Has RangeFilter
NullValueRatio: 0.2
DistinctValueCount: 123
}
)"};
  EXPECT_EQ(stream.str(), expected_string);
}

}  // namespace hyrise
