#pragma once

#include <compare>
#include <type_traits>

namespace hyrise {

template <typename Comparator, typename T>
concept BooleanComparator = requires(Comparator comparator, const T& lhs, const T& rhs) {
                              { comparator(lhs, rhs) } -> std::same_as<bool>;
                            };

template <typename Comparator, typename T>
concept ThreeWayComparator = requires(Comparator comparator, const T& lhs, const T& rhs) {
                               { comparator(lhs, rhs) } -> std::convertible_to<std::partial_ordering>;
                             };

}  // namespace hyrise
