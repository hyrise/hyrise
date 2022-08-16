#include "date_time_utils.hpp"

#include <magic_enum.hpp>

namespace hyrise {

std::optional<boost::posix_time::ptime> string_to_timestamp(const std::string& timestamp_string) {
  // We catch parsing exceptions since we return a std::nullopt if the input string is not a valid timestamp.
  try {
    if (timestamp_string.size() == 10) {
      // This is a date without time information.
      const auto date = boost::gregorian::from_simple_string(timestamp_string);
      if (!date.is_not_a_date()) {
        return boost::posix_time::ptime{date};
      }
    } else {
      const auto timestamp = boost::posix_time::time_from_string(timestamp_string);
      if (!timestamp.is_not_a_date_time()) {
        return timestamp;
      }
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
      // We obtain an iterator that adds offset years when incremented. Thus, we have to actually increment and
      // dereference it. The same applies to the other cases.
      return *(++boost::gregorian::year_iterator{start_date, static_cast<int>(offset)});
    }
    case DatetimeComponent::Month: {
      return *(++boost::gregorian::month_iterator{start_date, static_cast<int>(offset)});
    }
    case DatetimeComponent::Day: {
      return *(++boost::gregorian::day_iterator{start_date, static_cast<int>(offset)});
    }
    default:
      Fail("Invalid time unit for date interval: " + std::string{magic_enum::enum_name(unit)});
  }

  Fail("Invalid enum value");
}

std::string date_to_string(const boost::gregorian::date& date) {
  return boost::gregorian::to_iso_extended_string(date);
}

std::string timestamp_to_string(const boost::posix_time::ptime& timestamp) {
  auto string_representation = boost::posix_time::to_iso_extended_string(timestamp);
  // Transform '2000-01-01T00:00:00' to '2000-01-01 00:00:00'.
  string_representation.replace(10, 1, " ");
  return string_representation;
}

}  // namespace hyrise
