#pragma once

#include "types.hpp"

#include <boost/date_time/gregorian/gregorian.hpp>

namespace opossum {

std::optional<boost::gregorian::date> string_to_date(const std::string& date_string);

boost::gregorian::date date_interval(const boost::gregorian::date& start_date, int64_t offset, DatetimeComponent unit);

std::string date_to_string(const boost::gregorian::date& date);

}  // namespace opossum
