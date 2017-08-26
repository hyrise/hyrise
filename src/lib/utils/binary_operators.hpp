#pragma once

#include <functional>

#include <types.hpp>

namespace opossum {

template <typename Functor>
void resolve_operator_type(const ScanType scan_type, const Functor& func) {
  switch (scan_type) {
    case ScanType::OpEquals:
      func(std::equal_to<void>{});
      break;

    case ScanType::OpNotEquals:
      func(std::not_equal_to<void>{});
      break;

    case ScanType::OpLessThan:
      func(std::less<void>{});
      break;

    case ScanType::OpLessThanEquals:
      func(std::less_equal<void>{});
      break;

    case ScanType::OpGreaterThan:
      func(std::greater<void>{});
      break;

    case ScanType::OpGreaterThanEquals:
      func(std::greater_equal<void>{});
      break;

    case ScanType::OpBetween:
      Fail("This method should only be called when ScanType::OpBetween has been ruled out.");

    case ScanType::OpLike:
      Fail("This method should only be called when ScanType::OpLike has been ruled out.");

    default:
      Fail("Unsupported operator.");
  }
}

}  // namespace opossum
