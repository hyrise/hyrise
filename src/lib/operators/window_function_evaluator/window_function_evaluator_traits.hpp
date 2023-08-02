#pragma once

#include <limits>
#include <type_traits>

#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "util.hpp"

namespace hyrise::window_function_evaluator {

constexpr static auto rank_like_window_functions =
    std::array{WindowFunction::Rank, WindowFunction::DenseRank, WindowFunction::RowNumber, WindowFunction::PercentRank};

constexpr bool is_rank_like(WindowFunction window_function) {
  return std::ranges::find(rank_like_window_functions, window_function) != rank_like_window_functions.end();
}

namespace impls {

template <typename Impl, typename OutputType>
concept IsOnePassImpl =
    requires {
      typename Impl::State;
      { Impl::initial_state(std::declval<const RelevantRowInformation&>()) } -> std::same_as<typename Impl::State>;
      // NOLINTNEXTLINE(readability/braces)
      { Impl::update_state(std::declval<typename Impl::State&>(), std::declval<const RelevantRowInformation&>()) };
      { Impl::current_value(std::declval<const typename Impl::State&>()) } -> std::same_as<OutputType>;
    };

template <typename Impl>
concept IsSegmentTreeImpl =
    requires {
      typename Impl::InputType;
      typename Impl::OutputType;
      typename Impl::TreeNode;
      { Impl::neutral_element } -> std::convertible_to<typename Impl::TreeNode>;
      { Impl::node_from_value(std::declval<typename Impl::InputType>()) } -> std::same_as<typename Impl::TreeNode>;
      {
        Impl::combine(std::declval<typename Impl::TreeNode>(), std::declval<typename Impl::TreeNode>())
        } -> std::same_as<typename Impl::TreeNode>;
      { Impl::transform_query(std::declval<typename Impl::TreeNode>()) } -> std::same_as<typename Impl::OutputType>;
    };

//////////////////////////////////////////////////////////////////////////////

template <typename InputT, typename OutputT>
struct TrivialSegmentTreeImpl {
  using InputType = InputT;
  using OutputType = OutputT;
  using TreeNode = OutputType;

  static TreeNode node_from_value(InputType input) {
    return input;
  }

  static OutputType transform_query(auto node) {
    return node;
  }
};

template <typename NonNullableImpl>
  requires IsSegmentTreeImpl<NonNullableImpl>
struct MakeNullableImpl {
  using InputType = std::optional<typename NonNullableImpl::InputType>;
  using OutputType = std::optional<typename NonNullableImpl::OutputType>;
  using TreeNode = std::optional<typename NonNullableImpl::TreeNode>;

  constexpr static TreeNode neutral_element = std::nullopt;

  static TreeNode node_from_value(InputType input) {
    if (input)
      return NonNullableImpl::node_from_value(*input);
    return std::nullopt;
  }

  static TreeNode combine(TreeNode lhs, TreeNode rhs) {
    if (!lhs && !rhs)
      return std::nullopt;
    return NonNullableImpl::combine(lhs.value_or(NonNullableImpl::neutral_element),
                                    rhs.value_or(NonNullableImpl::neutral_element));
  }

  static OutputType transform_query(TreeNode node) {
    if (node)
      return NonNullableImpl::transform_query(*node);
    return std::nullopt;
  }
};

//////////////////////////////////////////////////////////////////////////////

struct AllRanksOnePassBase {
  struct State {
    const RelevantRowInformation* previous_row;
    uint64_t rank;
    uint64_t dense_rank;
    uint64_t row_number;
  };

  static State initial_state(const RelevantRowInformation& row) {
    return State{
        .previous_row = &row,
        .rank = 1,
        .dense_rank = 1,
        .row_number = 1,
    };
  }

  static void update_state(State& state, const RelevantRowInformation& row) {
    const auto is_peer = std::is_eq(compare_with_null_equal(state.previous_row->order_values, row.order_values));
    ++state.row_number;
    if (!is_peer) {
      state.rank = state.row_number;
      ++state.dense_rank;
    }
    state.previous_row = &row;
  }
};

};  // namespace impls

//////////////////////////////////////////////////////////////////////////////

template <typename InputColumnTypeT, WindowFunction window_function>
struct WindowFunctionEvaluatorTraits {};

//////////////////////////////////////////////////////////////////////////////

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Rank> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Rank>::ReturnType;

  struct OnePassImpl : impls::AllRanksOnePassBase {
    static OutputColumnType current_value(const State& state) {
      return state.rank;
    }
  };
};

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::DenseRank> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::DenseRank>::ReturnType;

  struct OnePassImpl : impls::AllRanksOnePassBase {
    static OutputColumnType current_value(const State& state) {
      return state.dense_rank;
    }
  };
};

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::RowNumber> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::RowNumber>::ReturnType;

  struct OnePassImpl : impls::AllRanksOnePassBase {
    static OutputColumnType current_value(const State& state) {
      return state.row_number;
    }
  };
};

//////////////////////////////////////////////////////////////////////////////

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Min> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Min>::ReturnType;

  using BaseImpl = impls::TrivialSegmentTreeImpl<InputColumnType, OutputColumnType>;

  struct NonNullSegmentTreeImpl : BaseImpl {
    using TreeNode = typename BaseImpl::TreeNode;
    constexpr static TreeNode neutral_element = std::numeric_limits<TreeNode>::max();

    static TreeNode combine(TreeNode lhs, TreeNode rhs) {
      return std::min(lhs, rhs);
    }
  };

  using NullableSegmentTreeImpl = impls::MakeNullableImpl<NonNullSegmentTreeImpl>;
};

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Max> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Max>::ReturnType;

  using BaseImpl = impls::TrivialSegmentTreeImpl<InputColumnType, OutputColumnType>;

  struct NonNullSegmentTreeImpl : BaseImpl {
    using TreeNode = typename BaseImpl::TreeNode;

    constexpr static TreeNode neutral_element = std::numeric_limits<TreeNode>::min();

    static TreeNode combine(TreeNode lhs, TreeNode rhs) {
      return std::max(lhs, rhs);
    }
  };

  using NullableSegmentTreeImpl = impls::MakeNullableImpl<NonNullSegmentTreeImpl>;
};

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Count> {
  using InputColumnType = InputColumnTypeT;

  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Count>::ReturnType;

  struct NullableSegmentTreeImpl {
    using InputType = std::optional<InputColumnType>;
    using OutputType = OutputColumnType;

    // The node contains the count of non-null values
    using TreeNode = OutputColumnType;

    constexpr static TreeNode neutral_element = 0;

    static TreeNode node_from_value(InputType input) {
      return input.has_value();
    }

    static TreeNode combine(TreeNode lhs, TreeNode rhs) {
      return lhs + rhs;
    }

    static OutputType transform_query(TreeNode node) {
      return node;
    }
  };
};

template <typename InputColumnTypeT>
  requires std::is_arithmetic_v<InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Sum> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Sum>::ReturnType;

  using BaseImpl = impls::TrivialSegmentTreeImpl<InputColumnType, OutputColumnType>;

  struct NonNullSegmentTreeImpl : BaseImpl {
    using TreeNode = typename BaseImpl::TreeNode;

    constexpr static TreeNode neutral_element = 0;

    static TreeNode combine(TreeNode lhs, TreeNode rhs) {
      return lhs + rhs;
    }
  };

  using NullableSegmentTreeImpl = impls::MakeNullableImpl<NonNullSegmentTreeImpl>;
};

template <typename InputColumnTypeT>
  requires std::is_arithmetic_v<InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Avg> {
  using InputColumnType = InputColumnTypeT;

  using SumT = typename WindowFunctionTraits<InputColumnType, WindowFunction::Sum>::ReturnType;
  using CountT = typename WindowFunctionTraits<InputColumnType, WindowFunction::Count>::ReturnType;
  using AvgT = typename WindowFunctionTraits<InputColumnType, WindowFunction::Avg>::ReturnType;

  using OutputColumnType = AvgT;

  struct NullableSegmentTreeImpl {
    using InputType = std::optional<InputColumnType>;
    using OutputType = std::optional<OutputColumnType>;

    struct TreeNode {
      SumT sum;
      CountT non_null_count;
    };

    constexpr static TreeNode neutral_element = TreeNode{.sum = 0, .non_null_count = 0};

    static TreeNode node_from_value(InputType input) {
      if (input) {
        return TreeNode{.sum = *input, .non_null_count = 1};
      }
      return neutral_element;
    }

    static TreeNode combine(TreeNode lhs, TreeNode rhs) {
      return TreeNode{.sum = lhs.sum + rhs.sum, .non_null_count = lhs.non_null_count + rhs.non_null_count};
    }

    static OutputType transform_query(TreeNode node) {
      if (node.non_null_count == 0)
        return std::nullopt;
      return static_cast<AvgT>(node.sum) / static_cast<AvgT>(node.non_null_count);
    }
  };
};

//////////////////////////////////////////////////////////////////////////////

template <typename InputColumnType, WindowFunction window_function>
concept SupportsOnePass =
    requires {
      typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::OnePassImpl;
      requires impls::IsOnePassImpl<
          typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::OnePassImpl,
          typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::OutputColumnType>;
    };

template <typename InputColumnType, WindowFunction window_function>
concept SupportsSegmentTree =
    requires {
      typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::NullableSegmentTreeImpl;
      requires impls::IsSegmentTreeImpl<
          typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::NullableSegmentTreeImpl>;
      requires std::is_same_v<
          std::optional<InputColumnType>,
          typename WindowFunctionEvaluatorTraits<InputColumnType, window_function>::NullableSegmentTreeImpl::InputType>;
    };

};  // namespace hyrise::window_function_evaluator
