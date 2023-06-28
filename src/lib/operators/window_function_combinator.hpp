#pragma once

#include <optional>

#include "abstract_aggregate_operator.hpp"
#include "aggregate/window_function_traits.hpp"

namespace hyrise {

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

};  // namespace hyrise
