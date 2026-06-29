#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <functional>
#include <limits>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <optional>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aggregate_dyod_utils/ticketing.hpp"
#include "all_type_variant.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "operators/abstract_aggregate_operator.hpp"
#include "operators/abstract_operator.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

// Computes a single aggregate over the whole table (no group-by). Returns the value and whether it is NULL. An
// aggregate over zero contributing (non-NULL) values is NULL, except COUNT which is 0.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::pair<AggregateType, bool> _aggregate_all_values(const std::shared_ptr<const Table>& input_table,
                                                     const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto accumulator = AggregateType{};
  auto value_count = size_t{0};
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      aggregate_function(position.value(), value_count, accumulator);
      ++value_count;
    });
  }

  // Finalize aggregates that cannot be computed incrementally per row.
  if constexpr (window_function == WindowFunction::Count) {
    // The COUNT lambda is a no-op; the actual count is the number of non-NULL values seen.
    return {static_cast<AggregateType>(value_count), false};
  } else if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    // AVG reuses the SUM accumulator and must be divided by the number of contributing values.
    if (value_count == 0) {
      return {AggregateType{}, true};
    }
    return {accumulator / static_cast<AggregateType>(value_count), false};
  } else {
    // MIN/MAX/SUM/ANY: NULL when no non-NULL value contributed.
    return {accumulator, value_count == 0};
  }
}

// STDDEV_SAMP needs a used a different accumulator than the other aggregates (Welford's thingy...),
// so it gets its own scan helpers instead of reusing `_aggregate_all_values`.
// The result is NULL whenever fewer than two values contribute.
template <typename ColumnDataType>
std::optional<double> _standard_deviation_sample_all_values(const std::shared_ptr<const Table>& input_table,
                                                            const ColumnID input_column_id) {
  static_assert(std::is_arithmetic_v<ColumnDataType>, "StandardDeviationSample is only defined on arithmetic types.");
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, double, WindowFunction::StandardDeviationSample>().get_aggregate_function();
  auto accumulator = StandardDeviationSampleData{};
  auto value_count = size_t{0};
  const auto chunk_count = input_table->chunk_count();

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      aggregate_function(position.value(), value_count, accumulator);
      ++value_count;
    });
  }

  // STDDEV_SAMP is undefined (NULL) for fewer than two contributing values.
  if (value_count < 2) {
    return std::nullopt;
  }
  return accumulator[3];
}

// COUNT(DISTINCT) needs to know how many distinct non-NULL values each group has, which the incremental
// `WindowFunctionBuilder` accumulator cannot track. We therefore collect the distinct values per group in a set.
template <typename ColumnDataType>
int64_t _count_distinct_all_values(const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  auto distinct_values = std::unordered_set<ColumnDataType>{};
  const auto chunk_count = input_table->chunk_count();
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& segment = input_table->get_chunk(chunk_id)->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
      if (position.is_null()) {
        return;
      }
      distinct_values.insert(position.value());
    });
  }
  return static_cast<int64_t>(distinct_values.size());
}

// Incrementally computable aggregates (MIN/MAX/SUM/AVG/COUNT), indexed per group. A group with no contributing
// (non-NULL) value yields NULL, except COUNT which yields 0.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::pair<pmr_vector<AggregateType>, pmr_vector<bool>> _aggregate_grouped(
    const std::vector<uint64_t>& tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto values = pmr_vector<AggregateType>(group_count);

  auto value_counts = std::vector<size_t>(group_count, 0);
  auto nulls = pmr_vector<bool>(group_count, window_function != WindowFunction::Count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    segment_iterate<ColumnDataType>(*aggregate_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++row_index;
        return;
      }
      const auto value = position.value();
      const auto ticket = tickets[row_index++];
      if constexpr (window_function == WindowFunction::Avg) {
        aggregate_function(value, value_counts[ticket], values[ticket]);
        ++value_counts[ticket];
        nulls[ticket] = false;
      } else if constexpr (window_function == WindowFunction::Count) {
        values[ticket]++;
      } else {
        // MIN/MAX/SUM: the aggregate function uses `aggregate_count == 0` to detect the first contributing value of a
        // group, so we must pass (and advance) the running per-group count rather than a constant 0.
        aggregate_function(value, value_counts[ticket], values[ticket]);
        ++value_counts[ticket];
        nulls[ticket] = false;
      }
    });
  }

  // We have aggregated all values per group, but need to apply some 'post-processing' to finalize the results.
  if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    for (auto ticket = size_t{0}; ticket < group_count; ++ticket) {
      if (value_counts[ticket] != 0) {
        values[ticket] = values[ticket] / static_cast<AggregateType>(value_counts[ticket]);
      }
    }
  }
  return {std::move(values), std::move(nulls)};
}

// COUNT(DISTINCT): number of distinct non-NULL values. Never NULL (0 for an all-NULL group).
template <typename ColumnDataType>
pmr_vector<int64_t> _count_distinct_grouped(const std::vector<uint64_t>& tickets, const size_t group_count,
                                            const std::shared_ptr<const Table>& input_table,
                                            const ColumnID input_column_id) {
  auto distinct_values = std::vector<std::unordered_set<ColumnDataType>>(group_count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*aggregate_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++row_index;
        return;
      }
      const auto value = position.value();
      distinct_values[tickets[row_index++]].insert(value);
    });
  }

  auto values = pmr_vector<int64_t>(group_count);
  for (auto i = size_t{0}; i < group_count; ++i) {
    values[i] = static_cast<int64_t>(distinct_values[i].size());
  }
  return values;
}

// ANY: the first value seen per group, NULL included (The value is passed through. All-NULL groups stay).
template <typename ColumnDataType>
std::pair<pmr_vector<ColumnDataType>, pmr_vector<bool>> _any_grouped(const std::vector<uint64_t>& tickets,
                                                                     const size_t group_count,
                                                                     const std::shared_ptr<const Table>& input_table,
                                                                     const ColumnID input_column_id) {
  auto seen = std::vector<bool>(group_count, false);
  auto values = pmr_vector<ColumnDataType>(group_count);
  auto nulls = pmr_vector<bool>(group_count, false);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    segment_iterate<ColumnDataType>(*aggregate_segment, [&](const auto& position) {
      const auto index = tickets[row_index++];
      if (seen[index]) {
        return;
      }
      seen[index] = true;
      if (position.is_null()) {
        nulls[index] = true;
      } else {
        values[index] = position.value();
      }
    });
  }
  return {std::move(values), std::move(nulls)};
}

// STDDEV: NULL for groups with fewer than two contributing values.
template <typename ColumnDataType>
std::pair<pmr_vector<double>, pmr_vector<bool>> _standard_deviation_sample_grouped(
    const std::vector<uint64_t>& tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  static_assert(std::is_arithmetic_v<ColumnDataType>, "StandardDeviationSample is only defined on arithmetic types.");
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, double, WindowFunction::StandardDeviationSample>().get_aggregate_function();
  auto accumulators = std::vector<StandardDeviationSampleData>(group_count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    segment_iterate<ColumnDataType>(*aggregate_segment, [&](const auto& position) {
      if (position.is_null()) {
        ++row_index;
        return;
      }
      const auto value = position.value();
      // Welford's algorithm tracks its own count in `accumulator[0]`, so the `aggregate_count` argument is unused.
      aggregate_function(value, size_t{0}, accumulators[tickets[row_index++]]);
    });
  }

  auto values = pmr_vector<double>(group_count);
  auto nulls = pmr_vector<bool>(group_count, false);
  for (auto i = size_t{0}; i < group_count; ++i) {
    if (accumulators[i][0] < 2) {
      nulls[i] = true;
    } else {
      values[i] = accumulators[i][3];
    }
  }
  return {std::move(values), std::move(nulls)};
}

std::shared_ptr<const Table> AggregateDYOD::_on_execute() {
  const auto input_table = _left_input->get_output();

  _validate_aggregates();

  if (_groupby_column_ids.empty()) {
    // Produces only a single per aggregate.

    const auto aggregate_count = _aggregates.size();

    auto column_definitions = TableColumnDefinitions{};
    auto result_values = std::vector<AllTypeVariant>{};
    column_definitions.reserve(aggregate_count);
    result_values.reserve(aggregate_count);

    for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
      const auto& aggregate = _aggregates[aggregate_id];

      const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
      const auto input_column_id = pqp_column.column_id;

      if (aggregate->window_function == WindowFunction::Any) {
        // ANY() passes the source column through, keeping its name, data type, and nullability.
        column_definitions.emplace_back(input_table->column_name(input_column_id),
                                        input_table->column_data_type(input_column_id),
                                        input_table->column_is_nullable(input_column_id));
      } else if (aggregate->window_function == WindowFunction::Count ||
                 aggregate->window_function == WindowFunction::CountDistinct) {
        // COUNT and COUNT DISTINCT never produce NULL values.
        column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), false);
      } else {
        column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);
      }

      const auto data_type =
          input_column_id == INVALID_COLUMN_ID ? DataType::Long : input_table->column_data_type(input_column_id);
      resolve_data_type(data_type, [&](const auto data_type_t) {
        using ColumnDataType = typename decltype(data_type_t)::type;

        switch (aggregate->window_function) {
          case WindowFunction::Min: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
            const auto [value, is_null] =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Min>(input_table, input_column_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Max: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
            const auto [value, is_null] =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Max>(input_table, input_column_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Sum: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
            const auto [value, is_null] =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Sum>(input_table, input_column_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Avg: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
            const auto [value, is_null] =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Avg>(input_table, input_column_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Count: {
            // Special case for COUNT(*): count all rows, ignoring the input column id.
            if (input_column_id == INVALID_COLUMN_ID) {
              auto value_count = size_t{0};
              const auto chunk_count = input_table->chunk_count();
              for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
                value_count += input_table->get_chunk(chunk_id)->size();
              }
              result_values.emplace_back(static_cast<int64_t>(value_count));
              break;
            } else {
              using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
              const auto [value, _] = _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Count>(
                  input_table, input_column_id);
              result_values.emplace_back(value);
              break;
            }
          }
          case WindowFunction::CountDistinct: {
            const auto result = _count_distinct_all_values<ColumnDataType>(input_table, input_column_id);
            result_values.emplace_back(result);
            break;
          }
          case WindowFunction::StandardDeviationSample: {
            if constexpr (std::is_arithmetic_v<ColumnDataType>) {
              const auto result = _standard_deviation_sample_all_values<ColumnDataType>(input_table, input_column_id);
              result_values.emplace_back(result ? AllTypeVariant{*result} : NULL_VALUE);
            } else {
              Fail("StandardDeviationSample is not available on non-arithmetic types.");
            }
            break;
          }
          case WindowFunction::Any: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::ReturnType;
            const auto [value, is_null] =
                _aggregate_all_values<ColumnDataType, AggregateType, WindowFunction::Any>(input_table, input_column_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          default:
            Fail(std::format("Unsupported aggregate function '{}'.",
                             window_function_to_string.left.at(aggregate->window_function)));
        }
      });
    }

    auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
    result_table->append(result_values);
    return result_table;
  }

  // Group-by path. We determine the distinct groups once and then derive every output column (the group-by columns
  // and each aggregate) from that shared, index-aligned structure, so all columns line up row-for-row.
  const auto aggregate_count = _aggregates.size();
  const auto groupby_column_count = _groupby_column_ids.size();

  auto groups = _compute_groups(_groupby_column_ids, input_table);
  const auto group_count = groups.group_count;

  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_count + aggregate_count);

  // The output schema is [group-by columns..., aggregate columns...]. The group-by output columns are already built by
  // the grouping phase. The aggregate columns are appended below.
  for (const auto groupby_column_id : _groupby_column_ids) {
    column_definitions.emplace_back(input_table->column_name(groupby_column_id),
                                    input_table->column_data_type(groupby_column_id),
                                    input_table->column_is_nullable(groupby_column_id));
  }
  auto output_segments = std::move(groups.groupby_segments);
  output_segments.reserve(groupby_column_count + aggregate_count);

  for (auto aggregate_id = uint32_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;

    if (window_function == WindowFunction::Any) {
      // ANY() is a pass-through of a column that is functionally dependent on the group-by columns. The output
      // therefore keeps the source column's name, data type, and nullability rather than the "ANY(...)" name.
      column_definitions.emplace_back(input_table->column_name(input_column_id),
                                      input_table->column_data_type(input_column_id),
                                      input_table->column_is_nullable(input_column_id));
    } else if (window_function == WindowFunction::Count || window_function == WindowFunction::CountDistinct) {
      // COUNT and COUNT DISTINCT never produce NULL.
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), false);
    } else {
      // All other aggregates can produce NULL.
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), true);
    }

    // COUNT(*) does not reference an input column. It counts all rows per group (NULLs included).
    // Therefore, we can just emit the per-group `row_counts` from the grouping result.
    if (window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID) {
      auto values = pmr_vector<int64_t>(group_count);
      for (auto i = size_t{0}; i < group_count; ++i) {
        values[i] = static_cast<int64_t>(groups.row_counts[i]);
      }
      output_segments.emplace_back(std::make_shared<ValueSegment<int64_t>>(std::move(values)));
      continue;
    }

    resolve_data_type(input_table->column_data_type(input_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      switch (window_function) {
        case WindowFunction::Min: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Min>(
              groups.tickets, group_count, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Max>(
              groups.tickets, group_count, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Sum>(
              groups.tickets, group_count, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Avg>(
              groups.tickets, group_count, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Count: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
          auto result = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Count>(
              groups.tickets, group_count, input_table, input_column_id);
          // COUNT never produces NULL.
          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result.first)));
          break;
        }
        case WindowFunction::CountDistinct: {
          auto values =
              _count_distinct_grouped<ColumnDataType>(groups.tickets, group_count, input_table, input_column_id);
          output_segments.emplace_back(std::make_shared<ValueSegment<int64_t>>(std::move(values)));
          break;
        }
        case WindowFunction::StandardDeviationSample: {
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            auto [values, nulls] = _standard_deviation_sample_grouped<ColumnDataType>(groups.tickets, group_count,
                                                                                      input_table, input_column_id);
            output_segments.emplace_back(std::make_shared<ValueSegment<double>>(std::move(values), std::move(nulls)));
          } else {
            Fail("StandardDeviationSample is not available on non-arithmetic types.");
          }
          break;
        }
        case WindowFunction::Any: {
          auto [values, nulls] =
              _any_grouped<ColumnDataType>(groups.tickets, group_count, input_table, input_column_id);
          // ANY() passes the source column through, so the output keeps its nullability.
          if (input_table->column_is_nullable(input_column_id)) {
            output_segments.emplace_back(
                std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls)));
          } else {
            output_segments.emplace_back(std::make_shared<ValueSegment<ColumnDataType>>(std::move(values)));
          }
          break;
        }
        default:
          Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
      }
    });
  }

  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  if (group_count > 0) {
    result_table->append_chunk(output_segments);
  }
  return result_table;
}

std::shared_ptr<AbstractOperator> AggregateDYOD::_on_deep_copy(
    const std::shared_ptr<AbstractOperator>& copied_left_input,
    const std::shared_ptr<AbstractOperator>& /*copied_right_input*/,
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& /*copied_ops*/) const {
  return std::make_shared<AggregateDYOD>(copied_left_input, _aggregates, _groupby_column_ids);
}

void AggregateDYOD::_on_set_parameters(const std::unordered_map<ParameterID, AllTypeVariant>& parameters) {}

void AggregateDYOD::_on_cleanup() {}

}  // namespace hyrise
