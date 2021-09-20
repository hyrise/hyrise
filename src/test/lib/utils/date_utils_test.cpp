#include "base_test.hpp"

#include "utils/date_utils.hpp"

namespace opossum {

class DateUtilsTest : public BaseTest {};

TEST_F(DateUtilsTest, StringToDate) {
  EXPECT_EQ(string_to_date("2000-01-45"), std::nullopt);
  EXPECT_EQ(string_to_date("2000-13-01"), std::nullopt);
  EXPECT_EQ(string_to_date("2000-04-31"), std::nullopt);
  EXPECT_EQ(string_to_date("2001-02-29"), std::nullopt);
  EXPECT_EQ(string_to_date("-1-02-29"), std::nullopt);
  EXPECT_EQ(string_to_date("foo"), std::nullopt);

  const auto parsed_date = string_to_date("2000-01-31");
  EXPECT_NE(parsed_date, std::nullopt);
  EXPECT_EQ(parsed_date->year(), 2000);
  EXPECT_EQ(parsed_date->month(), 1);
  EXPECT_EQ(parsed_date->day(), 31);

  const auto leap_year_date = string_to_date("2000-02-29");
  EXPECT_NE(leap_year_date, std::nullopt);
  EXPECT_EQ(leap_year_date->year(), 2000);
  EXPECT_EQ(leap_year_date->month(), 2);
  EXPECT_EQ(leap_year_date->day(), 29);
}

TEST_F(DateUtilsTest, DateInterval) {
  const boost::gregorian::date date{2000, 1, 31};
  const boost::gregorian::date leap_year_date{2000, 2, 29};
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Second), std::logic_error);
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Minute), std::logic_error);
  EXPECT_THROW(date_interval(date, 0, DatetimeComponent::Hour), std::logic_error);
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Day), (boost::gregorian::date{2000, 2, 1}));
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Month), leap_year_date);
  EXPECT_EQ(date_interval(date, 1, DatetimeComponent::Year), (boost::gregorian::date{2001, 1, 31}));
  EXPECT_EQ(date_interval(leap_year_date, 1, DatetimeComponent::Year), (boost::gregorian::date{2001, 2, 28}));
}

TEST_F(DateUtilsTest, DateToString) {
  const boost::gregorian::date date{2000, 1, 31};
  EXPECT_EQ(date_to_string(date), "2000-01-31");
}

}  // namespace opossum
