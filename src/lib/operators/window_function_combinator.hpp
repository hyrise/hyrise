#pragma once

#include <algorithm>
#include <limits>
#include <optional>
#include <type_traits>

#include "abstract_aggregate_operator.hpp"
#include "aggregate/window_function_traits.hpp"

namespace hyrise {

constexpr static auto segment_tree_window_functions =
    std::array{WindowFunction::Sum, WindowFunction::Min, WindowFunction::Max};

template <typename T, WindowFunction window_function>
concept UseSegmentTree = std::is_arithmetic_v<T> && std::ranges::find(segment_tree_window_functions, window_function) !=
segment_tree_window_functions.end();

template <typename T, WindowFunction window_function>
struct WindowFunctionCombinator {};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Sum> {
  using Combine = decltype([](std::optional<typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType> lhs,
                              std::optional<typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType> rhs)
                               -> std::optional<typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType> {
    if (!lhs && !rhs)
      return std::nullopt;
    return lhs.value_or(0) + rhs.value_or(0);
  });
  constexpr static auto neutral_element = std::optional<T>();
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Min> {
  using Combine = decltype([](std::optional<typename WindowFunctionTraits<T, WindowFunction::Min>::ReturnType> lhs,
                              std::optional<typename WindowFunctionTraits<T, WindowFunction::Min>::ReturnType> rhs)
                               -> std::optional<typename WindowFunctionTraits<T, WindowFunction::Min>::ReturnType> {
    if (!lhs && !rhs)
      return std::nullopt;
    return std::min(lhs.value_or(std::numeric_limits<T>::max()), rhs.value_or(std::numeric_limits<T>::max()));
  });
  constexpr static auto neutral_element = std::optional<T>();
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Max> {
  using Combine = decltype([](std::optional<typename WindowFunctionTraits<T, WindowFunction::Max>::ReturnType> lhs,
                              std::optional<typename WindowFunctionTraits<T, WindowFunction::Max>::ReturnType> rhs)
                               -> std::optional<typename WindowFunctionTraits<T, WindowFunction::Max>::ReturnType> {
    if (!lhs && !rhs)
      return std::nullopt;
    return std::max(lhs.value_or(std::numeric_limits<T>::min()), rhs.value_or(std::numeric_limits<T>::min()));
  });
  constexpr static auto neutral_element = std::optional<T>();
};

};  // namespace hyrise
