#include "util.hpp"

namespace hyrise::window_function_evaluator {

std::weak_ordering reverse(std::weak_ordering ordering) {
  if (ordering == std::weak_ordering::less) {
    return std::weak_ordering::greater;
  }
  if (ordering == std::weak_ordering::greater) {
    return std::weak_ordering::less;
  }
  return ordering;
}

std::weak_ordering compare_with_null_equal(const AllTypeVariant& lhs, const AllTypeVariant& rhs) {
  if (variant_is_null(lhs) && variant_is_null(rhs)) {
    return std::weak_ordering::equivalent;
  }
  if (variant_is_null(lhs)) {
    return std::weak_ordering::less;
  }
  if (variant_is_null(rhs)) {
    return std::weak_ordering::greater;
  }
  if (lhs < rhs) {
    return std::weak_ordering::less;
  }
  if (lhs == rhs) {
    return std::weak_ordering::equivalent;
  }
  return std::weak_ordering::greater;
}

std::weak_ordering compare_with_null_equal(std::span<const AllTypeVariant> lhs, std::span<const AllTypeVariant> rhs) {
  return compare_with_null_equal(lhs, rhs, []([[maybe_unused]] const auto column_index) { return false; });
}

bool RelevantRowInformation::is_peer_of(const RelevantRowInformation& other) const {
  return std::is_eq(compare_with_null_equal(order_values, other.order_values));
}

}  // namespace hyrise::window_function_evaluator
