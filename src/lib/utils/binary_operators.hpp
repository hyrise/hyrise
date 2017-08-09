#pragma once

#include <types.hpp>

namespace opossum {

struct Less {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs < rhs;
  }
};

struct LessEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs <= rhs;
  }
};

struct Greater {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs > rhs;
  }
};

struct GreaterEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs >= rhs;
  }
};

struct Equal {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs == rhs;
  }
};

struct NotEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType& lhs, const RightType& rhs) const {
    return lhs != rhs;
  }
};

template <typename Functor>
void resolve_operator_type(const ScanType scan_type, const Functor& func) {
  switch (scan_type) {
    case ScanType::OpEquals:
      func(Equal{});
      break;

    case ScanType::OpNotEquals:
      func(NotEqual{});
      break;

    case ScanType::OpLessThan:
      func(Less{});
      break;

    case ScanType::OpLessThanEquals:
      func(LessEqual{});
      break;

    case ScanType::OpGreaterThan:
      func(Greater{});
      break;

    case ScanType::OpGreaterThanEquals:
      func(GreaterEqual{});
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
