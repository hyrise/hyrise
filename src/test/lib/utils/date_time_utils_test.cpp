#include "base_test.hpp"

#include "utils/date_time_utils.hpp"

namespace hyrise {

class DateTimeUtilsTest : public BaseTest {};

TEST_F(DateTimeUtilsTest, StringToDate) {
  EXPECT_EQ(string_to_timestamp("2000-01-45"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-13-01"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-04-31"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2001-02-29"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("-1-02-29"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("foo"), std::nullopt);

  const auto parsed_date = string_to_timestamp("2000-01-31");
  EXPECT_NE(parsed_date, std::nullopt);
  EXPECT_EQ(parsed_date->date().year(), 2000);
  EXPECT_EQ(parsed_date->date().month(), 1);
  EXPECT_EQ(parsed_date->date().day(), 31);
  EXPECT_EQ(parsed_date->time_of_day().hours(), 0);
  EXPECT_EQ(parsed_date->time_of_day().minutes(), 0);
  EXPECT_EQ(parsed_date->time_of_day().seconds(), 0);
  EXPECT_EQ(parsed_date->time_of_day().fractional_seconds(), 0);

  const auto leap_year_date = string_to_timestamp("2000-02-29");
  EXPECT_NE(leap_year_date, std::nullopt);
  EXPECT_EQ(leap_year_date->date().year(), 2000);
  EXPECT_EQ(leap_year_date->date().month(), 2);
  EXPECT_EQ(leap_year_date->date().day(), 29);
}

TEST_F(DateTimeUtilsTest, StringToDateTime) {
  EXPECT_EQ(string_to_timestamp("2000-01-45 00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-13-01 00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-04-31 00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2001-02-29 00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("-1-02-29 00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-01-01T00:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-01-01 00:00:x"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-01-01 00:x:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("2000-01-01 x:00:00"), std::nullopt);
  EXPECT_EQ(string_to_timestamp("foo"), std::nullopt);

  const auto timestamp_without_microseconds = string_to_timestamp("2000-01-31 12:13:14");
  EXPECT_NE(timestamp_without_microseconds, std::nullopt);
  EXPECT_EQ(timestamp_without_microseconds->date().year(), 2000);
  EXPECT_EQ(timestamp_without_microseconds->date().month(), 1);
  EXPECT_EQ(timestamp_without_microseconds->date().day(), 31);
  EXPECT_EQ(timestamp_without_microseconds->time_of_day().hours(), 12);
  EXPECT_EQ(timestamp_without_microseconds->time_of_day().minutes(), 13);
  EXPECT_EQ(timestamp_without_microseconds->time_of_day().seconds(), 14);
  EXPECT_EQ(timestamp_without_microseconds->time_of_day().fractional_seconds(), 0);

  const auto timestamp_with_microseconds = string_to_timestamp("2000-01-31 12:13:14.5");
  EXPECT_NE(timestamp_with_microseconds, std::nullopt);
  EXPECT_EQ(timestamp_with_microseconds->date().year(), 2000);
  EXPECT_EQ(timestamp_with_microseconds->date().month(), 1);
  EXPECT_EQ(timestamp_with_microseconds->date().day(), 31);
  EXPECT_EQ(timestamp_with_microseconds->time_of_day().hours(), 12);
  EXPECT_EQ(timestamp_with_microseconds->time_of_day().minutes(), 13);
  EXPECT_EQ(timestamp_with_microseconds->time_of_day().seconds(), 14);
  const auto time_of_day = timestamp_with_microseconds->time_of_day();
  const auto fractional_seconds =
      static_cast<double>(time_of_day.fractional_seconds()) / static_cast<double>(time_of_day.ticks_per_second());
  EXPECT_EQ(fractional_seconds, 0.5);

  const auto timestamp_without_time = string_to_timestamp("2000-01-01 00:00:00");
  EXPECT_EQ(*string_to_timestamp("2000-01-01 00:"), *timestamp_without_time);
  EXPECT_EQ(*string_to_timestamp("2000-01-01 00:00"), *timestamp_without_time);
  EXPECT_EQ(*string_to_timestamp("2000-01-01 00:00:00.000000"), *timestamp_without_time);
  EXPECT_EQ(*string_to_timestamp("2000-01-01 0:0:0"), *timestamp_without_time);

  const auto timestamp_with_overflow = string_to_timestamp("2000-12-31 25:61:61");
  EXPECT_NE(timestamp_with_overflow, std::nullopt);
  EXPECT_EQ(timestamp_with_overflow->date().year(), 2001);
  EXPECT_EQ(timestamp_with_overflow->date().month(), 1);
  EXPECT_EQ(timestamp_with_overflow->date().day(), 1);
  EXPECT_EQ(timestamp_with_overflow->time_of_day().hours(), 2);
  EXPECT_EQ(timestamp_with_overflow->time_of_day().minutes(), 2);
  EXPECT_EQ(timestamp_with_overflow->time_of_day().seconds(), 1);
}

TEST_F(DateTimeUtilsTest, DateInterval) {
  const auto date = boost::gregorian::date{2000, 1, 31};
  const auto leap_year_date = boost::gregorian::date{2000, 2, 29};
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Second), std::logic_error);
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Minute), std::logic_error);
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Hour), std::logic_error);
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Day), (boost::gregorian::date{2000, 2, 1}));
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Month), leap_year_date);
  EXPECT_EQ(date_interval(boost::gregorian::date{1999, 11, 30}, 2, DatetimeComponent::Month), date);
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Year), (boost::gregorian::date{2001, 1, 31}));
  EXPECT_EQ(date_interval(leap_year_date, 1, DatetimeComponent::Year), (boost::gregorian::date{2001, 2, 28}));
  EXPECT_EQ(date_interval(boost::gregorian::date{1999, 2, 28}, 1, DatetimeComponent::Year), leap_year_date);
}

TEST_F(DateTimeUtilsTest, DateToString) {
  const auto date = boost::gregorian::date{2000, 1, 31};
  EXPECT_EQ(date_to_string(date), "2000-01-31");
}

TEST_F(DateTimeUtilsTest, TimestampToString) {
  const auto date = boost::gregorian::date{2000, 1, 31};
  const auto time_without_microseconds = boost::posix_time::time_duration{1, 1, 1, 0};
  const auto time_with_microseconds = boost::posix_time::time_duration{1, 1, 1, 500000};
  const auto timestamp_without_microseconds = boost::posix_time::ptime{date, time_without_microseconds};
  const auto timestamp_with_microseconds = boost::posix_time::ptime{date, time_with_microseconds};
  EXPECT_EQ(timestamp_to_string(timestamp_without_microseconds), "2000-01-31 01:01:01");
  EXPECT_EQ(timestamp_to_string(timestamp_with_microseconds), "2000-01-31 01:01:01.500000");
}

}  // namespace hyrise
