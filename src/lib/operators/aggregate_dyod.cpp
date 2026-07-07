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
#include "scheduler/abstract_task.hpp"
#include "scheduler/job_task.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Threshold that decides how the group-by output columns are built. When the input has at least this many rows per
// group (low cardinality), each group-by column is materialized by reading every group's value once from its distinct
// key row in the grouping hash table; below it (high cardinality), a sequential scan of the source column is cheaper.
// See `build_groupby_column` in `_on_execute`. This is a heuristic crossover and can be tuned.
constexpr auto GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP = size_t{4};

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

  // Only AVG needs a per-group count of contributing (non-NULL) values, for its final division. MIN/MAX/SUM detect
  // their first contributing value via `nulls`, and COUNT accumulates directly into `values`, so neither allocates it.
  auto value_counts = std::vector<size_t>(window_function == WindowFunction::Avg ? group_count : 0, 0);
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
        // MIN/MAX/SUM: the aggregate function only needs to know whether this is the group's first contributing value
        // (it checks `aggregate_count == 0`). `nulls[ticket]` is still true until that first value, so it doubles as
        // the first-seen flag and we avoid maintaining a separate per-group count.
        aggregate_function(value, nulls[ticket] ? size_t{0} : size_t{1}, values[ticket]);
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

// Builds one group-by output column by reading each group's representative value directly from its distinct key row in
// the grouping hash table. Every group appears exactly once as a hash-table key, so a single const pass over the table
// (`group_count` entries) yields all values without re-scanning the source column. Preferred for low-cardinality
// group-bys, where there are far fewer groups than input rows; otherwise the sequential scan in `_any_grouped` wins.
//
// `groupby_index` is the column's position among the group-by columns (its slot in the row's null bitmap and column
// offsets); `string_col_index` is its position among the string group-by columns (its heap string-pointer slot).
template <typename ColumnDataType, bool Concurrent>
std::pair<pmr_vector<ColumnDataType>, pmr_vector<bool>> _groupby_from_hash_table(
    const GroupKeyData<Concurrent>& group_key_data, const size_t group_count, const size_t groupby_index,
    const size_t string_col_index) {
  const auto& format = group_key_data.row_format;
  const auto& hash_table = group_key_data.global_hash_table;
  auto values = pmr_vector<ColumnDataType>(group_count);
  auto nulls = pmr_vector<bool>(group_count, false);
  const auto null_mask_bit = uint64_t{1} << groupby_index;

  // TODO(@forUnity): this may not compile because of auto& entry.
  const auto process_iterator = [&](const auto& entry) {
    const auto row_view = RowView{entry.first.row, format};
    const auto ticket = entry.second;

    // `stores_nulls` is only false when no group-by column is nullable, so `null_bitmap()` is only read when present.
    if (format.stores_nulls && (row_view.null_bitmap() & null_mask_bit)) {
      nulls[ticket] = true;
      return;
    }

    if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
      const auto length = row_view.string_length(groupby_index);
      if (length <= PREFIX_LENGTH) {
        // Short string: the whole value lives inline in the prefix.
        values[ticket] = pmr_string{row_view.string_prefix(groupby_index), length};
      } else {
        // Long string: the full, null-terminated value lives at the row's heap pointer.
        values[ticket] = pmr_string{row_view.string_ptr(string_col_index)};
      }
    } else {
      values[ticket] = row_view.read_value<ColumnDataType>(groupby_index);
    }
  };

  if constexpr (Concurrent) {
    hash_table.cvisit_all(process_iterator);
  } else {
    for (auto it = hash_table.cbegin(); it != hash_table.cend(); ++it) {
      process_iterator(*it);
    }
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

// Copies the half-open group range [begin, begin + length) of a full-length output column (one `ValueSegment` holding
// every group) into a new, chunk-sized `ValueSegment` of the same data type and nullability. The source range is read
// only, so slices of the same column for different chunks are independent.
std::shared_ptr<AbstractSegment> _slice_column(const AbstractSegment& column, const size_t begin, const size_t length) {
  auto slice = std::shared_ptr<AbstractSegment>{};
  resolve_data_type(column.data_type(), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    const auto& value_segment = static_cast<const ValueSegment<ColumnDataType>&>(column);

    const auto& values = value_segment.values();
    auto chunk_values = pmr_vector<ColumnDataType>(values.begin() + begin, values.begin() + begin + length);

    if (value_segment.is_nullable()) {
      const auto& nulls = value_segment.null_values();
      auto chunk_nulls = pmr_vector<bool>(nulls.begin() + begin, nulls.begin() + begin + length);
      slice = std::make_shared<ValueSegment<ColumnDataType>>(std::move(chunk_values), std::move(chunk_nulls));
    } else {
      slice = std::make_shared<ValueSegment<ColumnDataType>>(std::move(chunk_values));
    }
  });
  return slice;
}

// Splits the full-length output columns (one `ValueSegment` per column, each holding all `group_count` groups) into
// TARGET_CHUNK_SIZE-sized chunks, returning one segment list per output chunk ready for `Table::append_chunk`.
//
// Every (chunk, column) slice is produced from a disjoint, read-only input range and written to its own output slot, so
// the nested loop carries no cross-iteration dependencies: once we go multi-threaded it can be dispatched to a
// threadpool over output chunks (the outer loop) without any synchronization.
std::vector<pmr_vector<std::shared_ptr<AbstractSegment>>> _split_into_chunks(
    const pmr_vector<std::shared_ptr<AbstractSegment>>& columns, const size_t group_count) {
  const auto column_count = columns.size();
  const auto output_chunk_count = (group_count + TARGET_CHUNK_SIZE - 1) / TARGET_CHUNK_SIZE;

  auto output_chunks = std::vector<pmr_vector<std::shared_ptr<AbstractSegment>>>(
      output_chunk_count, pmr_vector<std::shared_ptr<AbstractSegment>>(column_count));

  // Parallelization point: this outer loop over output chunks is embarrassingly parallel.
  for (auto chunk_index = size_t{0}; chunk_index < output_chunk_count; ++chunk_index) {
    const auto begin = chunk_index * TARGET_CHUNK_SIZE;
    const auto this_chunk_size = std::min(static_cast<size_t>(TARGET_CHUNK_SIZE), group_count - begin);

    for (auto column_index = size_t{0}; column_index < column_count; ++column_index) {
      output_chunks[chunk_index][column_index] = _slice_column(*columns[column_index], begin, this_chunk_size);
    }
  }

  return output_chunks;
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

  const auto THREAD_COUNT =
      Hyrise::get().topology.num_cpus() - 1;  // TODO(@forUnity): decide this elsewhere and make sure this is correct
  const auto CONCURRENT = THREAD_COUNT > 1;
  std::shared_ptr<GroupKeyDataBase> groups;
  std::shared_ptr<GroupKeyData<true>> concurrent_groups;
  std::shared_ptr<GroupKeyData<false>> nonconcurrent_groups;
  if (CONCURRENT) {
    concurrent_groups = _compute_groups<true>(_groupby_column_ids, input_table);
    groups = concurrent_groups;
  } else {
    nonconcurrent_groups = _compute_groups<false>(_groupby_column_ids, input_table);
    groups = nonconcurrent_groups;
  }
  const auto group_count = groups->group_count;

  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_count + aggregate_count);

  // The output schema is [group-by columns..., aggregate columns...]. Here we only define the columns; the group-by
  // output segments are filled below (either from the fast path or via ticket-pass jobs) and the aggregate segments
  // by their own jobs.
  for (const auto groupby_column_id : _groupby_column_ids) {
    column_definitions.emplace_back(input_table->column_name(groupby_column_id),
                                    input_table->column_data_type(groupby_column_id),
                                    input_table->column_is_nullable(groupby_column_id));
  }

  // Output layout: the group-by columns occupy the first `groupby_column_count` slots, followed by one slot per
  // aggregate. Every job writes a fixed, disjoint slot, so none of them touch a shared, growing container.
  auto output_segments = pmr_vector<std::shared_ptr<AbstractSegment>>(groupby_column_count + aggregate_count);

  // Build the aggregate column definitions serially (cheap metadata lookups). This must not run inside the
  // per-aggregate jobs below, as they would race on `column_definitions`.
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
  }

  // Each aggregate column is computed independently from the shared grouping structure (`groups->tickets`) and
  // input table, and writes into its own `output_segments` slot. There are no cross-column dependencies, so we compute
  // one aggregate per job.
  const auto compute_aggregate = [&](const uint32_t aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;
    const auto target_index = groupby_column_count + aggregate_id;

    // COUNT(*) does not reference an input column. It counts all rows per group (NULLs included). Every input row
    // contributes its group's ticket exactly once, so the per-group count is just a histogram over the tickets.
    if (window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID) {
      auto values = pmr_vector<int64_t>(group_count, 0);
      for (const auto ticket : groups->tickets) {
        ++values[ticket];
      }
      output_segments[target_index] = std::make_shared<ValueSegment<int64_t>>(std::move(values));
      return;
    }

    resolve_data_type(input_table->column_data_type(input_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      switch (window_function) {
        case WindowFunction::Min: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Min>(
              groups->tickets, group_count, input_table, input_column_id);
          output_segments[target_index] =
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls));
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Max>(
              groups->tickets, group_count, input_table, input_column_id);
          output_segments[target_index] =
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls));
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Sum>(
              groups->tickets, group_count, input_table, input_column_id);
          output_segments[target_index] =
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls));
          break;
        }
        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Avg>(
              groups->tickets, group_count, input_table, input_column_id);
          output_segments[target_index] =
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls));
          break;
        }
        case WindowFunction::Count: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
          auto result = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Count>(
              groups->tickets, group_count, input_table, input_column_id);
          // COUNT never produces NULL.
          output_segments[target_index] = std::make_shared<ValueSegment<AggregateType>>(std::move(result.first));
          break;
        }
        case WindowFunction::CountDistinct: {
          auto values =
              _count_distinct_grouped<ColumnDataType>(groups->tickets, group_count, input_table, input_column_id);
          output_segments[target_index] = std::make_shared<ValueSegment<int64_t>>(std::move(values));
          break;
        }
        case WindowFunction::StandardDeviationSample: {
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            auto [values, nulls] = _standard_deviation_sample_grouped<ColumnDataType>(groups->tickets, group_count,
                                                                                      input_table, input_column_id);
            output_segments[target_index] = std::make_shared<ValueSegment<double>>(std::move(values), std::move(nulls));
          } else {
            Fail("StandardDeviationSample is not available on non-arithmetic types.");
          }
          break;
        }
        case WindowFunction::Any: {
          auto [values, nulls] =
              _any_grouped<ColumnDataType>(groups->tickets, group_count, input_table, input_column_id);
          // ANY() passes the source column through, so the output keeps its nullability.
          if (input_table->column_is_nullable(input_column_id)) {
            output_segments[target_index] =
                std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls));
          } else {
            output_segments[target_index] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
          }
          break;
        }
        default:
          Fail(std::format("Unsupported aggregate function '{}'.", window_function_to_string.left.at(window_function)));
      }
    });
  };

  // For low-cardinality group-bys (far fewer groups than input rows), each group-by column is cheaper to build by
  // reading every group's value once from its distinct key row in the hash table than by scanning the whole source
  // column; above that ratio the scattered key-row access loses to a sequential source scan. Only the byte-row grouping
  // path exposes a hash table (`has_hash_table`); the single-column fast path recovers group-by values by scanning.
  const auto input_row_count = input_table->row_count();

  // TODO(@V1nce1): Right now the single column fast path has `has_hash_table` set to false, so it always uses
  // the sequential scan. We could change that.
  const auto use_hash_table_for_groupby =
      groups->has_hash_table && group_count * GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP <= input_row_count;

  // Builds one group-by output column. Every row in a group carries the same group-by value, so we only need one value
  // per group. Depending on cardinality (`use_hash_table_for_groupby`) we either read it from the group's hash-table
  // key row or recover it with a sequential ANY scan of the source column (the first row seen per group wins).
  const auto build_groupby_column = [&](const uint32_t groupby_index) {
    const auto groupby_column_id = _groupby_column_ids[groupby_index];
    resolve_data_type(input_table->column_data_type(groupby_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      auto [values, nulls] = [&]() -> std::pair<pmr_vector<ColumnDataType>, pmr_vector<bool>> {
        if (!use_hash_table_for_groupby) {
          // High cardinality: a sequential scan of the source column beats chasing the scattered key rows.
          return _any_grouped<ColumnDataType>(groups->tickets, group_count, input_table, groupby_column_id);
        }
        // Low cardinality: read each group's value straight from its hash-table key row. `string_col_index` locates
        // this column among the string group-by columns (see `RowView::string_ptr`).
        auto string_col_index = size_t{0};
        for (auto index = uint32_t{0}; index < groupby_index; ++index) {
          if (input_table->column_data_type(_groupby_column_ids[index]) == DataType::String) {
            ++string_col_index;
          }
        }
        if (CONCURRENT) {
          return _groupby_from_hash_table<ColumnDataType, true>(*concurrent_groups, group_count, groupby_index,
                                                                string_col_index);
        } else {
          return _groupby_from_hash_table<ColumnDataType, false>(*nonconcurrent_groups, group_count, groupby_index,
                                                                 string_col_index);
        }
      }();

      if (input_table->column_is_nullable(groupby_column_id)) {
        output_segments[groupby_index] =
            std::make_shared<ValueSegment<ColumnDataType>>(std::move(values), std::move(nulls));
      } else {
        output_segments[groupby_index] = std::make_shared<ValueSegment<ColumnDataType>>(std::move(values));
      }
    });
  };

  // One job per output column: build each group-by column and compute each aggregate. They all read the
  // shared, read-only grouping structure and input table and write disjoint output slots, so there are no
  // dependencies between them. With fewer than two units we run inline to avoid the scheduling overhead.
  const auto unit_count = groupby_column_count + aggregate_count;
  const auto run_unit = [&](const size_t unit) {
    if (unit < groupby_column_count) {
      build_groupby_column(static_cast<uint32_t>(unit));
    } else {
      compute_aggregate(static_cast<uint32_t>(unit - groupby_column_count));
    }
  };

  if (unit_count < 2) {
    for (auto unit = size_t{0}; unit < unit_count; ++unit) {
      run_unit(unit);
    }
  } else {
    auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
    jobs.reserve(unit_count);
    for (auto unit = size_t{0}; unit < unit_count; ++unit) {
      jobs.emplace_back(std::make_shared<JobTask>([&run_unit, unit]() {
        run_unit(unit);
      }));
    }
    Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
  }

  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  if (group_count > 0) {
    // Each output column was accumulated as a single full-length segment. Split them into TARGET_CHUNK_SIZE-sized
    // chunks in one final pass (`_split_into_chunks` is structured so this can later run on a threadpool).
    auto output_chunks = _split_into_chunks(output_segments, group_count);
    for (auto& chunk_segments : output_chunks) {
      result_table->append_chunk(std::move(chunk_segments));
    }
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
