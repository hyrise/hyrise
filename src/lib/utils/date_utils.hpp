#pragma once

#include "types.hpp"

#include <boost/date_time/gregorian/gregorian.hpp>

namespace opossum {

/**
 *  Takes delimited date string with order year-month-day (ISO 8601 extended format), e.g., "2001-01-01".
 *  Notably, Boost's gregorian dates do not support years < 1400 or > 9999.
 */
std::optional<boost::gregorian::date> string_to_date(const std::string& date_string);

/**
 * This handles edge cases with days that are the end of a month.
 * E.g., March 31 + one month == April 30, and vice versa.
 * This includes also applies to leap years.
 */
boost::gregorian::date date_interval(const boost::gregorian::date& start_date, int64_t offset, DatetimeComponent unit);

// ISO 8601 extended format representation of the date
std::string date_to_string(const boost::gregorian::date& date);

}  // namespace opossum
