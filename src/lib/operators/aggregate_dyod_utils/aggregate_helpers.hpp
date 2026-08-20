#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <format>
#include <memory>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include "expression/window_function_expression.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod_utils/ticketing.hpp"
#include "storage/chunk.hpp"

namespace hyrise {

/** Type-erased base class for the chunked vectors used to build output columns. */
class BaseChunkedVector {
 public:
  BaseChunkedVector() = default;
  BaseChunkedVector(const BaseChunkedVector&) = default;
  BaseChunkedVector(BaseChunkedVector&&) = default;
  BaseChunkedVector& operator=(const BaseChunkedVector&) = default;
  BaseChunkedVector& operator=(BaseChunkedVector&&) = default;

  virtual ~BaseChunkedVector() = default;
};

/** A vector split into Hyrise-sized chunks. */
template <typename T>
class ChunkedVector : public BaseChunkedVector {
 public:
  static constexpr auto CHUNK_SIZE = static_cast<size_t>(TARGET_CHUNK_SIZE);

  ChunkedVector() = default;

  explicit ChunkedVector(const size_t size) {
    chunks.reserve((size + CHUNK_SIZE - 1) / CHUNK_SIZE);
    for (auto begin = size_t{0}; begin < size; begin += CHUNK_SIZE) {
      chunks.emplace_back(std::min(CHUNK_SIZE, size - begin));
    }
  }

  // This will normally return T& but not if T is bool. Bit packing makes it return a proxy object.
  decltype(auto) operator[](const size_t index) {
    return chunks[index / CHUNK_SIZE][index % CHUNK_SIZE];
  }

  // Kept public because output-building jobs fill the chunks directly.
  std::vector<pmr_vector<T>> chunks;
};

/** Moves a completed chunked column into the output chunks. */
template <typename T>
void emit_output_column(ChunkedVector<T> values, ChunkedVector<bool> nulls, const bool nullable,
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

/** State for MIN/MAX/SUM/AVG/COUNT/ANY. NULL input values never contribute. */
template <typename ColumnDataType, WindowFunction window_function>
class RegularAggregateState {
 public:
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, window_function>::ReturnType;

  void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk, const ColumnID input_column_id) {
    const auto aggregate_function =
        WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
    const auto& segment = chunk->get_segment(input_column_id);

    // A dictionary holds exactly the segment's distinct non-NULL values in ascending order, so its first/last entry
    // already is the segment's minimum/maximum and the rows need not be scanned.
    if constexpr (window_function == WindowFunction::Min || window_function == WindowFunction::Max) {
      if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<ColumnDataType>>(segment)) {
        const auto& dictionary = *dictionary_segment->dictionary();
        if (!dictionary.empty()) {
          aggregate_function(window_function == WindowFunction::Min ? dictionary.front() : dictionary.back(),
                             value_count, accumulator);
          ++value_count;
        }
        return;
      }
    }

    with_string_segment_iterate<ColumnDataType>(segment,
                                                [&](const auto& value, const bool is_null, const auto /*needs_copy*/) {
                                                  if (is_null) {
                                                    return;
                                                  }
                                                  // MIN/MAX/ANY use `value_count` to detect their first contributing
                                                  // value, the other aggregates ignore it.
                                                  aggregate_function(value, value_count, accumulator);
                                                  ++value_count;
                                                });
  }

  void merge(RegularAggregateState& other_state) {
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
  std::pair<AggregateType, bool> finalize() const {
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

/** State for COUNT(DISTINCT), which counts distinct non-NULL values. */
template <typename ColumnDataType>
class CountDistinctAggregateState {
 public:
  using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::ReturnType;

  void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk, const ColumnID input_column_id) {
    const auto& segment = chunk->get_segment(input_column_id);
    with_string_segment_iterate<ColumnDataType>(segment,
                                                [&](const auto& value, const bool is_null, const auto /*needs_copy*/) {
                                                  if (is_null) {
                                                    return;
                                                  }
                                                  distinct_values.emplace(ColumnDataType{value});
                                                });
  }

  void merge(CountDistinctAggregateState& other) {
    distinct_values.merge(other.distinct_values);
  }

  std::pair<AggregateType, bool> finalize() const {
    return {static_cast<AggregateType>(distinct_values.size()), false};
  }

  std::unordered_set<ColumnDataType> distinct_values;
};

/** State for STDDEV_SAMP, which is NULL for fewer than two contributing values. */
template <typename ColumnDataType>
class StandardDeviationSampleAggregateState {
 public:
  using AggregateType =
      typename WindowFunctionTraits<ColumnDataType, WindowFunction::StandardDeviationSample>::ReturnType;

  void accumulate_entire_chunk(const std::shared_ptr<const Chunk>& chunk, const ColumnID input_column_id) {
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
  void merge(StandardDeviationSampleAggregateState& other) {
    const auto& other_data = other.standard_deviation;
    const auto other_count = other_data[0];
    if (other_count == 0.0) {
      return;
    }

    const auto count = standard_deviation[0];
    const auto combined_count = count + other_count;
    const auto delta = other_data[1] - standard_deviation[1];
    standard_deviation[2] += other_data[2] + (delta * delta * count * other_count / combined_count);
    standard_deviation[1] += delta * other_count / combined_count;
    standard_deviation[0] = combined_count;
    // `standard_deviation[3]` (the running result) is stale after merging. `finalize` recomputes it from the combined
    // count and squared distance.
  }

  std::pair<AggregateType, bool> finalize() const {
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
inline std::shared_ptr<void> make_no_groupby_aggregate_state(const DataType data_type,
                                                             const WindowFunction window_function) {
  auto state = std::shared_ptr<void>{};
  resolve_data_type(data_type, [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    resolve_window_function(window_function, [&](const auto window_function_t) {
      state = std::make_shared<IntermediateState<ColumnDataType, decltype(window_function_t)::value>>();
    });
  });
  return state;
}

}  // namespace hyrise
