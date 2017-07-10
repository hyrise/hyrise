#include "types.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::string scan_type_to_string(ScanType scan_type) {
  switch (scan_type) {
    case ScanType::OpEquals:
      return "=";
    case ScanType::OpNotEquals:
      return "!=";
    case ScanType::OpLessThan:
      return "<";
    case ScanType::OpLessThanEquals:
      return "<=";
    case ScanType::OpGreaterThan:
      return ">";
    case ScanType::OpGreaterThanEquals:
      return ">=";
    case ScanType::OpBetween:
      return "BETWEEN";
    case ScanType::OpLike:
      return "LIKE";
  }

  Fail("Unknown ScanType");
  return "";
}

}