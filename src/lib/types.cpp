#include "types.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::string scan_type_to_string(ScanType scan_type) {
  switch(scan_type) {
    case ScanType::OpEquals: return "=";
      case ScanType::OpNotEquals: return "!=";
      case ScanType::OpLessThan: return "<";
      case ScanType::OpLessThanEquals: return "<=";
      case ScanType::OpGreaterThan: return ">";
      case ScanType::OpGreaterThanEquals: return ">=";
      case ScanType::OpBetween: return "BETWEEN";
      case ScanType::OpLike: return "LIKE";
    default: // Covers OpUnknown
      break;
  }
  Fail("Unknown ScanType " + std::to_string(static_cast<std::underlying_type<ScanType>::type>(scan_type)));
  return "";
}

ScanType scan_type_from_string(const std::string & scan_str) {
  if (scan_str == "=") return ScanType::OpEquals;
  if (scan_str == "!=") return ScanType::OpNotEquals;
  if (scan_str == "<") return ScanType::OpLessThan;
  if (scan_str == "<=") return ScanType::OpLessThanEquals;
  if (scan_str == ">") return ScanType::OpGreaterThan;
  if (scan_str == ">=") return ScanType::OpGreaterThanEquals;
  if (scan_str == "BETWEEN") return ScanType::OpBetween;
  if (scan_str == "LIKE") return ScanType::OpLike;
  Fail("Unknown Scan '" + scan_str + "'");
  return ScanType::OpUnknown;
}

}