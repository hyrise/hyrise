#include "type_utils.hpp"

#include "assert.hpp"

namespace opossum {

ScanType flip_scan_type(ScanType scan_type) {
  DebugAssert(scan_type != ScanType::OpBetween, "Can't flip Between");
  DebugAssert(scan_type != ScanType::OpLike, "Can't flip Like");

  switch (scan_type) {
    case ScanType::OpEquals:
      return ScanType::OpEquals;
    case ScanType::OpNotEquals:
      return ScanType::OpNotEquals;
    case ScanType::OpLessThan:
      return ScanType::OpGreaterThan;
    case ScanType::OpLessThanEquals:
      return ScanType::OpGreaterThanEquals;
    case ScanType::OpGreaterThan:
      return ScanType::OpLessThan;
    case ScanType::OpGreaterThanEquals:
      return ScanType::OpLessThanEquals;
    case ScanType::OpBetween:
      return ScanType::OpBetween;  // Shouldn't be reached
    case ScanType::OpLike:
      return ScanType::OpLike;  // Shouldn't be reached
  }

  Fail("Shouldn't be reached");
  return ScanType::OpEquals; // stupid clang thinks this might get reached.
}
}