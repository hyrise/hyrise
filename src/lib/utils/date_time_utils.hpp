#pragma once

#include "types.hpp"

#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace opossum {

/**
 * Takes delimited date string with order year-month-day (ISO 8601 extended format), e.g., "2001-01-01".
 * Notably, Boost's gregorian dates do not support years < 1400 or > 9999.
 */
std::optional<boost::gregorian::date> string_to_date(const std::string& date_string);

/**
 * Takes delimited date string with order year-month-day hour:minute:second<.second fraction> (ISO 8601 extended
 * format), e.g., "2000-01-01 13:01:02" or "2000-01-01 13:01:02.5000". Boost allows out-of-bounds values for
 * timestamps, e.g., '2000-01-01 25:61:61' is a valid date time and the overflow is added to the subsequent time unit.
 * In this example, the resulting date time is '2000-01-02 02:02:01'. This behavior is enabled by Boost's time math,
 * see https://www.boost.org/doc/libs/1_79_0/doc/html/date_time/examples.html#date_time.examples.time_math
 */
std::optional<boost::posix_time::ptime> string_to_date_time(const std::string& date_time_string);

/**
 * This also handles edge cases with days that are the end of a month.
 * E.g., March 31 + one month == April 30, and vice versa.
 * This also applies to leap years.
 */
boost::gregorian::date date_interval(const boost::gregorian::date& start_date, int64_t offset, DatetimeComponent unit);

// ISO 8601 extended format representation of the date.
std::string date_to_string(const boost::gregorian::date& date);

// ISO 8601 extended format representation of the timestamp without time indicator.
std::string date_time_to_string(const boost::posix_time::ptime& date_time);

}  // namespace opossum
