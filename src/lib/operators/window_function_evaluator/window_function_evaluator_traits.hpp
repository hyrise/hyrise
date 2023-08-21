#pragma once

#include <limits>
#include <type_traits>

#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "util.hpp"

namespace hyrise::window_function_evaluator {

// This file contains all definitions of `WindowFunctionEvaluatorTraits`, a templated struct that has partial
// specializations for combinations of input column data type and window function containing the behavior needed to
// compute said window function in `WindowFunctionEvaluator::compute_window_function`. There are two computation
// strategies that can be used: `OnePass` and `SegmentTree`.
//
// The `impls` namespace contains generic helpers for implementing a strategy. After the `impls` namespace, there are
// concrete implementations of the strategies on the `WindowFunctionEvaluator` object.

namespace impls {

// The following two concepts define the requirements for an "implementation struct". Later, we will use these to check
// that a concrete `WindowFunctionEvaluatorTraits` type contains special member types adhering to these concepts. Note
// that both concepts require static functions on the implementation type that might appear to be more appropriately
// placed as member functions of another type. However, since we want to be able to use fundamental types for some of
// the required types, we do no require anything inside of the type's namespace.

// A one pass implementation can keep an arbitrary state during iteration. This state is constructed once for the first
// row of the partition and then updated once for each following row. Once a new partition starts, a new `State` is
// constructed from the initial row. For each row, the `current_value` according to the `State` is queried.
template <typename Impl, typename OutputType>
concept IsOnePassImpl =
    requires {
      typename Impl::State;
      { Impl::initial_state(std::declval<const RelevantRowInformation&>()) } -> std::same_as<typename Impl::State>;
      // NOLINTNEXTLINE(readability/braces)
      { Impl::update_state(std::declval<typename Impl::State&>(), std::declval<const RelevantRowInformation&>()) };
      { Impl::current_value(std::declval<const typename Impl::State&>()) } -> std::same_as<OutputType>;
    };

// A segment tree implementation must provide provide the `combine` operation and the neutral element for this operation
// (see `segment_tree.hpp` for an introduction to the data structure). Additionally, the implementation can choose to
// store more data than just the window function aggregate of the range in a tree node by setting `TreeNode` to
// something different than `OutputType` (this is used for example in the implementation of `WindowFunction::Avg`). In
// that case, there are two additional functions to transform a tuple's value to its corresponding leaf node and to
// transform an aggregate from the tree (which is of type `TreeNode`) back to the `OutputType`.
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

// Base class for segment tree implementations that do not need special transformations before and after querying the
// tree (for example `WindowFunction::Sum`). Instead, both transformations are defined as the identity function.
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

// An adapter implementation that takes an implementation and adds NULL handling to it. In particular, this resembles
// the NULL behavior of SQL aggregation: If all values are NULL, then the aggregation is NULL, otherwise, aggregate all
// non-NULL values.
template <typename NonNullableImpl>
  requires IsSegmentTreeImpl<NonNullableImpl>
struct MakeNullableImpl {
  using InputType = std::optional<typename NonNullableImpl::InputType>;
  using OutputType = std::optional<typename NonNullableImpl::OutputType>;
  using TreeNode = std::optional<typename NonNullableImpl::TreeNode>;

  constexpr static auto neutral_element = TreeNode{std::nullopt};

  static TreeNode node_from_value(InputType input) {
    if (input) {
      return NonNullableImpl::node_from_value(*input);
    }
    return std::nullopt;
  }

  static TreeNode combine(TreeNode lhs, TreeNode rhs) {
    if (!lhs && !rhs) {
      return std::nullopt;
    }
    return NonNullableImpl::combine(lhs.value_or(NonNullableImpl::neutral_element),
                                    rhs.value_or(NonNullableImpl::neutral_element));
  }

  static OutputType transform_query(TreeNode node) {
    if (node) {
      return NonNullableImpl::transform_query(*node);
    }
    return std::nullopt;
  }
};

// A base class for one pass implementations that tracks the three supported rank-like window functions all at once. For
// the concrete implementation of these window functions, only `transform_query` needs to be added so that it returns
// the correct member of `state`.
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
    ++state.row_number;
    if (!state.previous_row->is_peer_of(row)) {
      state.rank = state.row_number;
      ++state.dense_rank;
    }
    state.previous_row = &row;
  }
};

}  // namespace impls

// Template struct containing one pass or segment tree implementation types as dependent types. See the
// `SupportsOnePass` and `SupportsSegmentTree` concepts below.
template <typename InputColumnTypeT, WindowFunction window_function>
struct WindowFunctionEvaluatorTraits {};

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

template <typename InputColumnTypeT>
struct WindowFunctionEvaluatorTraits<InputColumnTypeT, WindowFunction::Min> {
  using InputColumnType = InputColumnTypeT;
  using OutputColumnType = typename WindowFunctionTraits<InputColumnType, WindowFunction::Min>::ReturnType;

  using BaseImpl = impls::TrivialSegmentTreeImpl<InputColumnType, OutputColumnType>;

  struct NonNullSegmentTreeImpl : BaseImpl {
    using TreeNode = typename BaseImpl::TreeNode;
    constexpr static auto neutral_element = std::numeric_limits<TreeNode>::max();

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

    constexpr static auto neutral_element = std::numeric_limits<TreeNode>::min();

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

    constexpr static auto neutral_element = TreeNode{0};

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

    constexpr static auto neutral_element = TreeNode{0};

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

    constexpr static auto neutral_element = TreeNode{.sum = 0, .non_null_count = 0};

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

// The following two concepts define the requirements for using the one pass or segment tree computation strategy in
// `WindowFunctionEvaluator::compute_window_function` respectively. Both concepts check for a dependent type that must
// pass the `IsOnePassImpl` or `IsSegmentTreeImpl` concepts. Note that the segment tree implementation explicitly
// requires NULL handling. As the entire step of `compute_window_function` does not take a significant amount of time at
// the moment, there is no way to add an optimized implementation for non-NULL input columns. However, such an
// optimization could be added by reusing the `IsSegmentTreeImpl` and using `T` instead of `std::optional<T>` as the
// `InputType`.

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

}  // namespace hyrise::window_function_evaluator
