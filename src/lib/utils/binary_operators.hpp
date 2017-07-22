#pragma once


namespace opossum {

struct Less {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs < rhs; }
};

struct LessEqual {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs <= rhs; }
};

struct Greater {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs > rhs; }
};

struct GreaterEqual {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs >= rhs; }
};

struct Equal {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs == rhs; }
};

struct NotEqual {
  auto operator()(const auto & lhs, const auto & rhs) { return lhs != rhs; }
};

}  // namespace opossum
