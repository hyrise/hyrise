#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "storage/chunk.hpp"

namespace hyrise {

struct BaseChunkedVector {
  virtual ~BaseChunkedVector() = default;
};

template <typename T>
struct ChunkedVector : public BaseChunkedVector {
  static constexpr auto CHUNK_SIZE = static_cast<size_t>(TARGET_CHUNK_SIZE);

  ChunkedVector() = default;

  explicit ChunkedVector(const size_t size, const T initial_value = T{}) {
    chunks.reserve((size + CHUNK_SIZE - 1) / CHUNK_SIZE);
    for (auto begin = size_t{0}; begin < size; begin += CHUNK_SIZE) {
      chunks.emplace_back(std::min(CHUNK_SIZE, size - begin), initial_value);
    }
  }

  // This will normally retunr T& but not if T is bool. There bit packing makes it return a proxy object.
  decltype(auto) operator[](const size_t index) {
    return chunks[index / CHUNK_SIZE][index % CHUNK_SIZE];
  }

  std::vector<pmr_vector<T>> chunks;
};

template <typename T>
void _emit_output_column(ChunkedVector<T>&& values, ChunkedVector<bool>&& nulls, const bool nullable,
                         std::vector<Segments>& output_chunks, const size_t column_index) {
  const auto chunk_count = values.chunks.size();
  for (auto chunk_index = size_t{0}; chunk_index < chunk_count; ++chunk_index) {
    if (nullable) {
      output_chunks[chunk_index][column_index] = std::make_shared<ValueSegment<T>>(
          std::move(values.chunks[chunk_index]), std::move(nulls.chunks[chunk_index]));
    } else {
      output_chunks[chunk_index][column_index] =
          std::make_shared<ValueSegment<T>>(std::move(values.chunks[chunk_index]));
    }
  }
}

template <typename ColumnDataType, WindowFunction window_function>
struct BaseAggregateState {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;

  virtual void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk, const ColumnID input_column_id) = 0;

  virtual void merge(BaseAggregateState<ColumnDataType, window_function>& other) = 0;

  virtual std::pair<AggregateType, bool> finalize() const = 0;

  virtual ~BaseAggregateState() = default;
};

// Incrementally computable aggregates (MIN/MAX/SUM/AVG/COUNT/ANY). NULL input values never contribute; an aggregate
// without a single contributing value yields NULL, except COUNT which yields 0.
template <typename ColumnDataType, WindowFunction window_function>
struct RegularAggregateState : public BaseAggregateState<ColumnDataType, window_function> {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;

  virtual void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk,
                                       const ColumnID input_column_id) override final {
    const auto aggregate_function =
        WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
    const auto& segment = chunk->get_segment(input_column_id);

    with_string_segment_iterate<ColumnDataType>(segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  if (is_null) {
                                                    return;
                                                  }
                                                  // MIN/MAX/ANY use `value_count` to detect their first contributing
                                                  // value, the other aggregates ignore it.
                                                  aggregate_function(value, value_count, accumulator);
                                                  ++value_count;
                                                });
  }

  virtual void merge(BaseAggregateState<ColumnDataType, window_function>& other) override final {
    const auto& other_state = static_cast<RegularAggregateState<ColumnDataType, window_function>&>(other);
    if (other_state.value_count == 0) {
      return;
    }

    if constexpr (window_function == WindowFunction::Min) {
      if (value_count == 0 || value_smaller(other_state.accumulator, accumulator)) {
        accumulator = other_state.accumulator;
      }
    } else if constexpr (window_function == WindowFunction::Max) {
      if (value_count == 0 || value_greater(other_state.accumulator, accumulator)) {
        accumulator = other_state.accumulator;
      }
    } else if constexpr (window_function == WindowFunction::Sum || window_function == WindowFunction::Avg) {
      accumulator += other_state.accumulator;
    } else if constexpr (window_function == WindowFunction::Any) {
      if (value_count == 0) {
        accumulator = other_state.accumulator;
      }
    }
    // COUNT derives its result from `value_count` alone, so there is nothing else to combine for it.
    value_count += other_state.value_count;
  }

  // Returns the aggregate's result value, or NULL if the aggregate is undefined for the values seen.
  virtual std::pair<AggregateType, bool> finalize() const override final {
    if constexpr (window_function == WindowFunction::Count) {
      // COUNT never produces NULL: an input without contributing values counts zero of them.
      return {static_cast<AggregateType>(value_count), false};
    } else {
      if (value_count == 0) {
        return {AggregateType{}, true};
      }
      // AVG accumulates the sum (see `WindowFunctionBuilder`) and only divides once, here.
      if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
        return {accumulator / static_cast<AggregateType>(value_count), false};
      } else {
        return {accumulator, false};
      }
    }
  }

  AggregateType accumulator{};
  size_t value_count{0};
};

// COUNT(DISTINCT): number of distinct non-NULL values. Never NULL (0 for an all-NULL input).
template <typename ColumnDataType>
struct CountDistinctAggregateState : public BaseAggregateState<ColumnDataType, WindowFunction::CountDistinct> {
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::ReturnType;

  virtual void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk,
                                       const ColumnID input_column_id) override final {
    const auto& segment = chunk->get_segment(input_column_id);
    with_string_segment_iterate<ColumnDataType>(segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  if (is_null) {
                                                    return;
                                                  }
                                                  distinct_values.emplace(ColumnDataType{value});
                                                });
  }

  virtual void merge(BaseAggregateState<ColumnDataType, WindowFunction::CountDistinct>& other) override final {
    distinct_values.merge(static_cast<CountDistinctAggregateState<ColumnDataType>&>(other).distinct_values);
  }

  virtual std::pair<AggregateType, bool> finalize() const override final {
    return {static_cast<AggregateType>(distinct_values.size()), false};
  }

  std::unordered_set<ColumnDataType> distinct_values;
};

// STDDEV_SAMP: NULL for fewer than two contributing values.
template <typename ColumnDataType>
struct StandardDeviationSampleAggregateState
    : public BaseAggregateState<ColumnDataType, WindowFunction::StandardDeviationSample> {
  using AggregateType =
      typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;

  virtual void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk,
                                       const ColumnID input_column_id) override final {
    if constexpr (std::is_arithmetic_v<ColumnDataType>) {
      const auto aggregate_function =
          WindowFunctionBuilder<ColumnDataType, AggregateType, WindowFunction::StandardDeviationSample>()
              .get_aggregate_function();
      const auto& segment = chunk->get_segment(input_column_id);
      segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
        if (!position.is_null()) {
          // Welford's algorithm tracks its own count in `standard_deviation[0]`, so `aggregate_count` is unused.
          aggregate_function(position.value(), size_t{0}, standard_deviation);
        }
      });
    } else {
      Fail("StandardDeviationSample is not available on non-arithmetic types.");
    }
  }

  // Combines two Welford states (Chan et al.'s parallel variance algorithm).
  virtual void merge(
      BaseAggregateState<ColumnDataType, WindowFunction::StandardDeviationSample>& other) override final {
    const auto& other_data =
        static_cast<StandardDeviationSampleAggregateState<ColumnDataType>&>(other).standard_deviation;
    const auto other_count = other_data[0];
    if (other_count == 0.0) {
      return;
    }

    const auto count = standard_deviation[0];
    const auto combined_count = count + other_count;
    const auto delta = other_data[1] - standard_deviation[1];
    standard_deviation[2] += other_data[2] + delta * delta * count * other_count / combined_count;
    standard_deviation[1] += delta * other_count / combined_count;
    standard_deviation[0] = combined_count;
    // `standard_deviation[3]` (the running result) is stale after merging. `finalize` recomputes it from the combined
    // count and squared distance.
  }

  virtual std::pair<AggregateType, bool> finalize() const override final {
    // The SQL standard defines STDDEV_SAMP as NULL for fewer than two values.
    if (standard_deviation[0] < 2.0) {
      return {AggregateType{}, true};
    }
    if constexpr (std::is_arithmetic_v<ColumnDataType>) {
      // The final result is the square root of the variance, which is the squared distance divided by (count - 1).
      return {std::sqrt(standard_deviation[2] / (standard_deviation[0] - 1.0)), false};
    } else {
      Fail("StandardDeviationSample is not available on non-arithmetic types.");
    }
  }

  StandardDeviationSampleData standard_deviation{};
};

// Resolves the runtime `window_function` to a compile - time constant and passes it to `functor` as an
// `std::integral_constant`, analogous to `resolve_data_type`.
template <typename Functor>
void resolve_window_function(const WindowFunction window_function, const Functor& functor) {
  switch (window_function) {
    case WindowFunction::Min:
      functor(std::integral_constant<WindowFunction, WindowFunction::Min>{});
      return;
    case WindowFunction::Max:
      functor(std::integral_constant<WindowFunction, WindowFunction::Max>{});
      return;
    case WindowFunction::Sum:
      functor(std::integral_constant<WindowFunction, WindowFunction::Sum>{});
      return;
    case WindowFunction::Avg:
      functor(std::integral_constant<WindowFunction, WindowFunction::Avg>{});
      return;
    case WindowFunction::Count:
      functor(std::integral_constant<WindowFunction, WindowFunction::Count>{});
      return;
    case WindowFunction::Any:
      functor(std::integral_constant<WindowFunction, WindowFunction::Any>{});
      return;
    case WindowFunction::CountDistinct:
      functor(std::integral_constant<WindowFunction, WindowFunction::CountDistinct>{});
      return;
    case WindowFunction::StandardDeviationSample:
      functor(std::integral_constant<WindowFunction, WindowFunction::StandardDeviationSample>{});
      return;
    default:
      Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
  }
}

// Maps a (column data type, window function) pair to the aggregate state implementing it. Most window functions are
// incrementally computable and share `RegularAggregateState`; COUNT(DISTINCT) and STDDEV_SAMP need their own state.
template <typename ColumnDataType, WindowFunction window_function>
struct IntermediateStateTraits {
  using Type = RegularAggregateState<ColumnDataType, window_function>;
};

template <typename ColumnDataType>
struct IntermediateStateTraits<ColumnDataType, WindowFunction::CountDistinct> {
  using Type = CountDistinctAggregateState<ColumnDataType>;
};

template <typename ColumnDataType>
struct IntermediateStateTraits<ColumnDataType, WindowFunction::StandardDeviationSample> {
  using Type = StandardDeviationSampleAggregateState<ColumnDataType>;
};

template <typename ColumnDataType, WindowFunction window_function>
using IntermediateState = typename IntermediateStateTraits<ColumnDataType, window_function>::Type;

// Per-aggregate information that both the aggregation jobs and the finalization need. It only depends on the aggregate
// expression and the input schema, so it is resolved once up front.
struct AggregateInfo {
  ColumnID input_column_id{INVALID_COLUMN_ID};
  DataType data_type{DataType::Long};
  WindowFunction window_function{WindowFunction::Min};
  bool is_count_star{false};
  // True for COUNT(*) and for COUNT on a non-nullable column (No GroupBy): every input row contributes, so the result
  // is the input's row count and no per-chunk work is needed at all. COUNT(*) also has no input column to scan
  // (`INVALID_COLUMN_ID`).
  bool counts_all_rows{false};
};

// Creates the aggregation state matching the aggregate's input data type and window function.
std::shared_ptr<void> _make_no_groupby_aggregate_state(const DataType data_type, const WindowFunction window_function) {
  auto state = std::shared_ptr<void>{};
  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    switch (window_function) {
      case WindowFunction::Min:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Min>>();
        break;
      case WindowFunction::Max:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Max>>();
        break;
      case WindowFunction::Sum:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Sum>>();
        break;
      case WindowFunction::Avg:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Avg>>();
        break;
      case WindowFunction::Count:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Count>>();
        break;
      case WindowFunction::Any:
        state = std::make_shared<RegularAggregateState<ColumnDataType, WindowFunction::Any>>();
        break;
      case WindowFunction::CountDistinct:
        state = std::make_shared<CountDistinctAggregateState<ColumnDataType>>();
        break;
      case WindowFunction::StandardDeviationSample:
        state = std::make_shared<StandardDeviationSampleAggregateState<ColumnDataType>>();
        break;
      default:
        Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
    }
  });
  return state;
}

std::shared_ptr<void> _make_global_aggregate_state(const WindowFunction window_function, const DataType data_type,
                                                   const size_t group_count) {
  auto state = std::shared_ptr<void>{};
  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    resolve_window_function(window_function, [&](const auto window_function_t) {
      const auto window_function = decltype(window_function_t)::value;

      if constexpr (window_function == WindowFunction::CountDistinct) {
        state = std::make_shared<std::vector<CountDistinctAggregateState<ColumnDataType>>>(group_count);
      } else if constexpr (window_function == WindowFunction::StandardDeviationSample) {
        state = std::make_shared<std::vector<StandardDeviationSampleAggregateState<ColumnDataType>>>(group_count);
      } else {
        state = std::make_shared<std::vector<RegularAggregateState<ColumnDataType, window_function>>>(group_count);
      }
    });
  });
  return state;
}

}  // namespace hyrise
