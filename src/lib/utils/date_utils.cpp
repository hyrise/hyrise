#include "date_utils.hpp"

#include <magic_enum.hpp>

namespace opossum {

std::optional<boost::gregorian::date> string_to_date(const std::string& date_string) {
  try {
    const auto date = boost::gregorian::from_string(date_string);
    if (date.is_not_a_date()) return std::nullopt;
    return date;
  } catch (const boost::wrapexcept<boost::gregorian::bad_day_of_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_year>&) {
  } catch (const boost::wrapexcept<boost::bad_lexical_cast>&) {
  }
  return std::nullopt;
}

boost::gregorian::date date_interval(const boost::gregorian::date& start_date, int64_t offset, DatetimeComponent unit) {
  switch (unit) {
    case DatetimeComponent::Year: {
      const boost::date_time::year_functor<boost::gregorian::date> interval(offset);
      return start_date + interval.get_offset(start_date);
    }
    case DatetimeComponent::Month: {
      const boost::date_time::month_functor<boost::gregorian::date> interval(offset);
      return start_date + interval.get_offset(start_date);
    }
    case DatetimeComponent::Day: {
      const boost::date_time::day_functor<boost::gregorian::date> interval(offset);
      return start_date + interval.get_offset(start_date);
    }
    default:
      Fail("Invalid time unit for date interval: " + std::string{magic_enum::enum_name(unit)});
  }
}

std::string date_to_string(const boost::gregorian::date& date) {
  return boost::gregorian::to_iso_extended_string(date);
}

}  // namespace opossum
