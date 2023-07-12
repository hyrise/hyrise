#pragma once

#include <algorithm>
#include <functional>
#include <limits>
#include <optional>
#include <type_traits>

#include "abstract_aggregate_operator.hpp"
#include "aggregate/window_function_traits.hpp"
#include "expression/window_function_expression.hpp"
#include "window_function_evaluator.hpp"

namespace hyrise {

template <typename T>
std::optional<T> as_optional(AllTypeVariant value) {
  if (variant_is_null(value))
    return std::nullopt;
  return get<T>(value);
}

constexpr static auto rank_like_window_functions =
    std::array{WindowFunction::Rank, WindowFunction::DenseRank, WindowFunction::RowNumber, WindowFunction::PercentRank};

constexpr bool is_rank_like(WindowFunction window_function) {
  return std::ranges::find(rank_like_window_functions, window_function) != rank_like_window_functions.end();
}

template <WindowFunction window_function>
concept RankLike = is_rank_like(window_function);

template <typename T, WindowFunction window_function>
struct WindowFunctionCombinator {};

template <typename T, typename Combinator>
struct OnePassStateFromTreeLogicMixin {
  // TODO(niklas): This currently assumes that tree nodes are optionals of the input value. Instead, the combinator should provide a `ToTreeNode` that replaces the current constructors

  struct OnePassState {
    using TreeNode = typename Combinator::TreeNode;

    TreeNode value = Combinator::neutral_element;

    OnePassState() = default;

    explicit OnePassState(const WindowFunctionEvaluator::RelevantRowInformation& initial_row)
        : value(as_optional<T>(initial_row.function_argument)) {}

    auto current_value() const {
      using Transformer = typename Combinator::QueryTransformer;

      return Transformer{}(value);
    }

    void update([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      using Combine = typename Combinator::Combine;

      value = Combine{}(value, as_optional<T>(current_value.function_argument));
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Rank> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::Rank>::ReturnType;

  struct OnePassState {
    ReturnType row_number = 1;
    ReturnType rank = 1;

    OnePassState() = default;

    explicit OnePassState([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& initial_row) {}

    std::optional<ReturnType> current_value() const {
      return rank;
    }

    void update(const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      ++row_number;
      if (std::is_neq(compare_with_null_equal(previous_value.order_values, current_value.order_values)))
        rank = row_number;
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::DenseRank> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::DenseRank>::ReturnType;

  struct OnePassState {
    ReturnType rank = 1;

    OnePassState() = default;

    explicit OnePassState([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& initial_row) {}

    std::optional<ReturnType> current_value() const {
      return rank;
    }

    void update(const WindowFunctionEvaluator::RelevantRowInformation& previous_value,
                const WindowFunctionEvaluator::RelevantRowInformation& current_value) {
      if (std::is_neq(compare_with_null_equal(previous_value.order_values, current_value.order_values)))
        ++rank;
    }
  };
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::RowNumber> {
  using ReturnType = typename WindowFunctionTraits<T, WindowFunction::RowNumber>::ReturnType;

  struct OnePassState {
    ReturnType row_number = 1;

    OnePassState() = default;

    explicit OnePassState([[maybe_unused]] const WindowFunctionEvaluator::RelevantRowInformation& initial_row) {}

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
struct WindowFunctionCombinator<T, WindowFunction::Sum>
    : OnePassStateFromTreeLogicMixin<T, WindowFunctionCombinator<T, WindowFunction::Sum>> {
  using SumReturnType = typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType;

  using TreeNode = std::optional<SumReturnType>;

  using Combine = decltype([](TreeNode lhs, TreeNode rhs) -> TreeNode {
    if (!lhs && !rhs)
      return std::nullopt;
    return lhs.value_or(0) + rhs.value_or(0);
  });
  constexpr static auto neutral_element = std::optional<SumReturnType>();

  using QueryTransformer = std::identity;
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Avg> {
  using SumT = typename WindowFunctionTraits<T, WindowFunction::Sum>::ReturnType;
  using CountT = typename WindowFunctionTraits<T, WindowFunction::Count>::ReturnType;

  struct TreeNode {
    SumT sum = 0;
    CountT non_null_count = 0;

    TreeNode() = default;

    TreeNode(SumT init_sum, CountT init_non_null_count) : sum(init_sum), non_null_count(init_non_null_count) {}

    constexpr explicit TreeNode(std::optional<T> value) {
      if (value) {
        sum = *value;
        non_null_count = 1;
      }
    }
  };

  using Combine = decltype([](TreeNode lhs, TreeNode rhs) {
    return TreeNode(lhs.sum + rhs.sum, lhs.non_null_count + rhs.non_null_count);
  });

  constexpr static auto neutral_element = TreeNode(std::nullopt);

  using AvgReturnType = typename WindowFunctionTraits<T, WindowFunction::Avg>::ReturnType;

  using QueryTransformer = decltype([](TreeNode query_result) -> std::optional<AvgReturnType> {
    if (query_result.non_null_count == 0)
      return std::nullopt;
    return static_cast<AvgReturnType>(query_result.sum) / static_cast<AvgReturnType>(query_result.non_null_count);
  });
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Min>
    : OnePassStateFromTreeLogicMixin<T, WindowFunctionCombinator<T, WindowFunction::Min>> {
  using MinReturnType = typename WindowFunctionTraits<T, WindowFunction::Min>::ReturnType;
  using TreeNode = std::optional<MinReturnType>;

  using Combine = decltype([](TreeNode lhs, TreeNode rhs) -> TreeNode {
    if (!lhs && !rhs)
      return std::nullopt;
    return std::min(lhs.value_or(std::numeric_limits<MinReturnType>::max()),
                    rhs.value_or(std::numeric_limits<MinReturnType>::max()));
  });
  constexpr static auto neutral_element = std::optional<MinReturnType>();

  using QueryTransformer = std::identity;
};

template <typename T>
struct WindowFunctionCombinator<T, WindowFunction::Max>
    : OnePassStateFromTreeLogicMixin<T, WindowFunctionCombinator<T, WindowFunction::Max>> {
  using MaxReturnType = typename WindowFunctionTraits<T, WindowFunction::Max>::ReturnType;
  using TreeNode = std::optional<MaxReturnType>;

  using Combine = decltype([](TreeNode lhs, TreeNode rhs) -> TreeNode {
    if (!lhs && !rhs)
      return std::nullopt;
    return std::max(lhs.value_or(std::numeric_limits<MaxReturnType>::min()),
                    rhs.value_or(std::numeric_limits<MaxReturnType>::min()));
  });
  constexpr static auto neutral_element = std::optional<MaxReturnType>();

  using QueryTransformer = std::identity;
};

template <typename T, WindowFunction window_function>
concept SupportsOnePass = requires { typename WindowFunctionCombinator<T, window_function>::OnePassState; };

template <typename T, WindowFunction window_function>
concept SupportsSegmentTree = requires {
                                typename WindowFunctionCombinator<T, window_function>::Combine;
                                typename WindowFunctionCombinator<T, window_function>::TreeNode;
                                { WindowFunctionCombinator<T, window_function>::neutral_element };
                              };

};  // namespace hyrise
