#pragma once

#include <string>
#include <type_traits>

namespace opossum {

// JoinHashTraits

template <typename L, typename R, class Enable = void>
struct JoinHashTraits {
  using HashType = void;
};

// If both are floating types, use the larger type to hash
template <typename L, typename R>
struct JoinHashTraits<L, R, std::enable_if_t<std::is_floating_point_v<L> && std::is_floating_point_v<R>>> {
  using HashType = std::conditional_t<sizeof(L) < sizeof(R), R, L>;  // NOLINT
};

// If both are integer types, use the larger type to hash
template <typename L, typename R>
struct JoinHashTraits<L, R, std::enable_if_t<std::is_integral_v<L> && std::is_integral_v<R>>> {
  using HashType = std::conditional_t<sizeof(L) < sizeof(R), R, L>;  // NOLINT
};

// If one is integer and the other floating type, use the floating type to hash
template <typename L, typename R>
struct JoinHashTraits<L, R,
                      std::enable_if_t<(std::is_integral_v<L> && std::is_floating_point_v<R>) ||
                                       (std::is_integral_v<R> && std::is_floating_point_v<L>)>> {
  using HashType = std::conditional_t<std::is_floating_point_v<L>, L, R>;
};

// Joining with strings will use strings for hashing and a lexical cast if necessary
template <typename L, typename R>
struct JoinHashTraits<L, R, std::enable_if_t<std::is_same_v<R, pmr_string> || std::is_same_v<L, pmr_string>>> {
  using HashType = pmr_string;
};

}  // namespace opossum
