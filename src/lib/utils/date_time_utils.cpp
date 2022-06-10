#include "date_time_utils.hpp"

#include <magic_enum.hpp>

namespace opossum {

std::optional<boost::gregorian::date> string_to_date(const std::string& date_string) {
  try {
    const auto date = boost::gregorian::from_string(date_string);
    if (!date.is_not_a_date()) {
      return date;
    }
  } catch (const boost::wrapexcept<boost::gregorian::bad_day_of_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_year>&) {
  } catch (const boost::wrapexcept<boost::bad_lexical_cast>&) {
  }
  return std::nullopt;
}

std::optional<boost::posix_time::ptime> string_to_date_time(const std::string& date_time_string) {
  try {
    const auto date_time = boost::posix_time::time_from_string(date_time_string);
    if (!date_time.is_not_a_date_time()) {
      return date_time;
    }
  } catch (const boost::wrapexcept<boost::gregorian::bad_day_of_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_month>&) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_year>&) {
  } catch (const boost::wrapexcept<boost::bad_lexical_cast>&) {
  } catch (const std::out_of_range&) {
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

std::string date_time_to_string(const boost::posix_time::ptime& date_time) {
  auto string_representation = boost::posix_time::to_iso_extended_string(date_time);
  // Transform '2000-01-01T00:00:00' to '2000-01-01 00:00:00'.
  string_representation.replace(10, 1, " ");
  return string_representation;
}

}  // namespace opossum
