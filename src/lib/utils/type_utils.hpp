#pragma once

#include <type_traits>

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Turns a scan_type so the expressions if the operands change sides as well, e.g., <= becomes >= and == remains ==
 */
ScanType flip_scan_type(ScanType scan_type);

/**
 * Shorthand for ColumnID{static_cast<ColumnID::base_type>(value)}
 */
template <typename T>
typename std::enable_if<std::is_integral<T>::value, ColumnID>::type make_column_id(T value) {
  DebugAssert(value >= 0 && value <= std::numeric_limits<ColumnID::base_type>::max(),
              "Value out of range for ColumnID");
  return ColumnID{static_cast<ColumnID::base_type>(value)};
}
}
