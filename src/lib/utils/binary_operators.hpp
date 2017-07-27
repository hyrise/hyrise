#pragma once


namespace opossum {

struct Less {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs < rhs; }
};

struct LessEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs <= rhs; }
};

struct Greater {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs > rhs; }
};

struct GreaterEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs >= rhs; }
};

struct Equal {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs == rhs; }
};

struct NotEqual {
  template <typename LeftType, typename RightType>
  auto operator()(const LeftType & lhs, const RightType & rhs) const { return lhs != rhs; }
};

}  // namespace opossum
