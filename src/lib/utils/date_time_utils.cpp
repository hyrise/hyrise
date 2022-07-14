#include "date_time_utils.hpp"

#include <magic_enum.hpp>

namespace opossum {

std::optional<boost::gregorian::date> string_to_date(const std::string& date_string) {
  // We catch parsing exceptions since we return a std::nullopt if the input string is not a valid date.
  try {
    const auto date = boost::gregorian::from_string(date_string);
    if (!date.is_not_a_date()) {
      return date;
    }
  } catch (const boost::wrapexcept<boost::gregorian::bad_day_of_month>& /* exception */) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_month>& /* exception */) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_year>& /* exception */) {
  } catch (const boost::wrapexcept<boost::bad_lexical_cast>& /* exception */) {}
  return std::nullopt;
}

std::optional<boost::posix_time::ptime> string_to_date_time(const std::string& date_time_string) {
  // We catch parsing exceptions since we return a std::nullopt if the input string is not a valid timestamp.
  try {
    const auto date_time = boost::posix_time::time_from_string(date_time_string);
    if (!date_time.is_not_a_date_time()) {
      return date_time;
    }
  } catch (const boost::wrapexcept<boost::gregorian::bad_day_of_month>& /* exception */) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_month>& /* exception */) {
  } catch (const boost::wrapexcept<boost::gregorian::bad_year>& /* exception */) {
  } catch (const boost::wrapexcept<boost::bad_lexical_cast>& /* exception */) {
  } catch (const std::out_of_range& /* exception */) {}
  return std::nullopt;
}

boost::gregorian::date date_interval(const boost::gregorian::date& start_date, int64_t offset, DatetimeComponent unit) {
  switch (unit) {
    case DatetimeComponent::Year: {
      return *(++boost::gregorian::year_iterator(start_date, offset));
    }
    case DatetimeComponent::Month: {
      return *(++boost::gregorian::month_iterator(start_date, offset));
    }
    case DatetimeComponent::Day: {
      return *(++boost::gregorian::day_iterator(start_date, offset));
    }
    default:
      Fail("Invalid time unit for date interval: " + std::string{magic_enum::enum_name(unit)});
  }

  Fail("Invalid enum value");
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
