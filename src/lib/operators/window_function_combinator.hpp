#pragma once

#include <algorithm>
#include <limits>
#include <optional>
#include <type_traits>

#include "abstract_aggregate_operator.hpp"
#include "aggregate/window_function_traits.hpp"
#include "expression/window_function_expression.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

constexpr static auto rank_like_window_functions =
    std::array{WindowFunction::Rank, WindowFunction::DenseRank, WindowFunction::RowNumber, WindowFunction::PercentRank};

constexpr bool is_rank_like(WindowFunction window_function) {
  return std::ranges::find(rank_like_window_functions, window_function) != rank_like_window_functions.end();
}

template <WindowFunction window_function>
concept RankLike = is_rank_like(window_function);

constexpr static auto segment_tree_window_functions =
    std::array{WindowFunction::Sum, WindowFunction::Min, WindowFunction::Max};

template <typename T, WindowFunction window_function>
concept UseSegmentTree = std::is_arithmetic_v<T> && std::ranges::find(segment_tree_window_functions, window_function) !=
segment_tree_window_functions.end();

template <typename T, WindowFunction window_function>
struct WindowFunctionCombinator {};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Rank> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::Rank>::ReturnType;

  struct OnePassState {
    ReturnType row_number = 1;
    ReturnType rank = 1;

    std::optional<ReturnType> current_value() const {
      return rank;
    }

    void update(const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      ++row_number;
      if (std::is_neq(WindowFunctionEvaluator::RelevantRowInformation::compare_with_null_equal(
              previous_value.order_values, current_value.order_values)))
        rank = row_number;
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::DenseRank> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::DenseRank>::ReturnType;

  struct OnePassState {
    ReturnType rank = 1;

    std::optional<ReturnType> current_value() const {
      return rank;
    }

    void update(const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      if (std::is_neq(WindowFunctionEvaluator::RelevantRowInformation::compare_with_null_equal(
              previous_value.order_values, current_value.order_values)))
        ++rank;
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::RowNumber> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::RowNumber>::ReturnType;

  struct OnePassState {
    ReturnType row_number = 1;

    std::optional<ReturnType> current_value() const {
      return row_number;
    }

    void update([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                [[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      ++row_number;
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Sum> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType;

  using Combine =
      decltype([](std::optional<ReturnType> lhs, std::optional<ReturnType> rhs) -> std::optional<ReturnType> {
        if (!lhs && !rhs)
          return std::nullopt;
        return lhs.value_or(0) + rhs.value_or(0);
      });
  constexpr static auto neutral_element = std::optional<T>();

  struct OnePassState {
    std::optional<ReturnType> sum{};

    std::optional<ReturnType> current_value() const {
      return sum;
    }

    void update([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      sum = Combine{}(sum, static_cast<ReturnType>(get<T>(current_value.function_argument)));
    }
  };
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

template <typename T, WindowFunction window_function>
concept SupportsOnePass = requires { typename WindowFunctionCombinator<T, window_function>::OnePassState; };

template <typename T, WindowFunction window_function>
concept SupportsSegmentTree = requires {
                                typename WindowFunctionCombinator<T, window_function>::Combine;
                                { WindowFunctionCombinator<T, window_function>::neutral_element };
                              };

};  // namespace hyrise
