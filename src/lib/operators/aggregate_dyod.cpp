#include "aggregate_dyod.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <format>
#include <memory>
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
#include "operators/aggregate/window_function_traits.hpp"
#include "resolve_type.hpp"
#include "scheduler/immediate_execution_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "storage/chunk.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

template <typename T>
struct ChunkedVector {
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

// Threshold that decides how the group-by output columns are built. When the input has at least this many rows per
// group (low cardinality), each group-by column is materialized by reading every group's value once from its distinct
// key row in the grouping hash table; below it (high cardinality), a sequential scan of the source column is cheaper.
// See `build_groupby_column` in `_on_execute`. This is a heuristic crossover and can be tuned.
constexpr auto GROUPBY_HASH_TABLE_MIN_ROWS_PER_GROUP = size_t{4};

// A type-erased partial result produced for one aggregate and one input chunk. Keeping the non-finalized state is
// important: AVG needs its sum and count, STDDEV_SAMP needs its Welford state, and COUNT DISTINCT needs the actual set
// of values in order to merge overlapping chunks correctly.
struct NoGroupbyPartialResult {
  AllTypeVariant accumulator{NULL_VALUE};
  size_t value_count{0};
  StandardDeviationSampleData standard_deviation{};
  std::unordered_set<AllTypeVariant> distinct_values;
};

template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
NoGroupbyPartialResult _aggregate_chunk(const std::shared_ptr<const Chunk>& chunk, const ColumnID input_column_id) {
  auto result = NoGroupbyPartialResult{};
  auto accumulator = AggregateType{};
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  const auto& segment = chunk->get_segment(input_column_id);

  with_string_segment_iterate<ColumnDataType>(segment,
                                              [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                if (is_null) {
                                                  return;
                                                }
                                                aggregate_function(value, result.value_count, accumulator);
                                                ++result.value_count;
                                              });
  if (result.value_count > 0) {
    result.accumulator = AllTypeVariant{accumulator};
  }
  return result;
}

template <typename ColumnDataType>
NoGroupbyPartialResult _count_distinct_chunk(const std::shared_ptr<const Chunk>& chunk,
                                             const ColumnID input_column_id) {
  auto result = NoGroupbyPartialResult{};
  const auto& segment = chunk->get_segment(input_column_id);
  with_string_segment_iterate<ColumnDataType>(segment,
                                              [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                if (is_null) {
                                                  return;
                                                }
                                                result.distinct_values.emplace(ColumnDataType{value});
                                              });
  return result;
}

template <typename ColumnDataType>
NoGroupbyPartialResult _standard_deviation_chunk(const std::shared_ptr<const Chunk>& chunk,
                                                 const ColumnID input_column_id) {
  auto result = NoGroupbyPartialResult{};
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, double, WindowFunction::StandardDeviationSample>().get_aggregate_function();
  const auto& segment = chunk->get_segment(input_column_id);
  segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
    if (!position.is_null()) {
      aggregate_function(position.value(), size_t{0}, result.standard_deviation);
    }
  });
  return result;
}

template <typename AggregateType, WindowFunction window_function>
std::pair<AggregateType, bool> _merge_no_groupby_partials(
    const std::vector<std::vector<NoGroupbyPartialResult>>& partial_results, const size_t aggregate_id) {
  auto accumulator = AggregateType{};
  auto value_count = size_t{0};

  for (const auto& chunk_results : partial_results) {
    const auto& partial = chunk_results[aggregate_id];
    if (partial.value_count == 0) {
      continue;
    }
    const auto& partial_value = boost::get<AggregateType>(partial.accumulator);
    if constexpr (window_function == WindowFunction::Min) {
      if (value_count == 0 || value_smaller(partial_value, accumulator)) {
        accumulator = partial_value;
      }
    } else if constexpr (window_function == WindowFunction::Max) {
      if (value_count == 0 || value_greater(partial_value, accumulator)) {
        accumulator = partial_value;
      }
    } else if constexpr (window_function == WindowFunction::Sum || window_function == WindowFunction::Avg) {
      accumulator += partial_value;
    } else if constexpr (window_function == WindowFunction::Any) {
      if (value_count == 0) {
        accumulator = partial_value;
      }
    } else if constexpr (window_function == WindowFunction::Count) {
      accumulator += static_cast<AggregateType>(partial.value_count);
    }
    value_count += partial.value_count;
  }

  if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
    if (value_count > 0) {
      accumulator /= static_cast<AggregateType>(value_count);
    }
  }
  if constexpr (window_function == WindowFunction::Count) {
    return {accumulator, false};
  }
  return {accumulator, value_count == 0};
}

std::optional<double> _merge_standard_deviation_partials(
    const std::vector<std::vector<NoGroupbyPartialResult>>& partial_results, const size_t aggregate_id) {
  auto count = 0.0;
  auto mean = 0.0;
  auto squared_distance = 0.0;
  for (const auto& chunk_results : partial_results) {
    const auto& partial = chunk_results[aggregate_id].standard_deviation;
    const auto partial_count = partial[0];
    if (partial_count == 0.0) {
      continue;
    }

    const auto combined_count = count + partial_count;
    const auto delta = partial[1] - mean;
    squared_distance += partial[2] + delta * delta * count * partial_count / combined_count;
    mean += delta * partial_count / combined_count;
    count = combined_count;
  }
  if (count < 2.0) {
    return std::nullopt;
  }
  return std::sqrt(squared_distance / (count - 1.0));
}

AggregateDYOD::AggregateDYOD(const std::shared_ptr<AbstractOperator>& input_operator,
                             const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                             const std::vector<ColumnID>& groupby_column_ids)
    : AbstractAggregateOperator(input_operator, aggregates, groupby_column_ids) {}

const std::string& AggregateDYOD::name() const {
  static const auto name = std::string{"AggregateDYOD"};
  return name;
}

// Incrementally computable aggregates (MIN/MAX/SUM/AVG/COUNT), indexed per group. A group with no contributing
// (non-NULL) value yields NULL, except COUNT which yields 0.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::pair<ChunkedVector<AggregateType>, ChunkedVector<bool>> _aggregate_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  auto values = ChunkedVector<AggregateType>(group_count);

  // Only AVG needs a per-group count of contributing (non-NULL) values, for its final division. MIN/MAX/SUM detect
  // their first contributing value via `nulls`, and COUNT accumulates directly into `values`, so neither allocates it.
  auto value_counts = std::vector<size_t>(window_function == WindowFunction::Avg ? group_count : 0, 0);
  auto nulls = ChunkedVector<bool>(group_count, window_function != WindowFunction::Count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    with_string_segment_iterate<ColumnDataType>(
        aggregate_segment, [&](const auto& value, const bool is_null, const auto needs_copy) {
          if (is_null) {
            ++row_index;
            return;
          }
          const auto ticket = tickets[row_index++];
          if constexpr (window_function == WindowFunction::Avg) {
            aggregate_function(value, value_counts[ticket], values[ticket]);
            ++value_counts[ticket];
            nulls[ticket] = false;
          } else if constexpr (window_function == WindowFunction::Count) {
            values[ticket]++;
          } else {
            // MIN/MAX/SUM: the aggregate function only needs to know whether this is the group's first contributing
            // value (it checks `aggregate_count == 0`). `nulls[ticket]` is still true until that first value, so it
            // doubles as the first-seen flag and we avoid maintaining a separate per-group count.
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
ChunkedVector<int64_t> _count_distinct_grouped(const uint64_t* const tickets, const size_t group_count,
                                               const std::shared_ptr<const Table>& input_table,
                                               const ColumnID input_column_id) {
  auto distinct_values = std::vector<std::unordered_set<ColumnDataType>>(group_count);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    with_string_segment_iterate<ColumnDataType>(aggregate_segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  if (is_null) {
                                                    ++row_index;
                                                    return;
                                                  }
                                                  distinct_values[tickets[row_index++]].insert(value);
                                                });
  }

  auto values = ChunkedVector<int64_t>(group_count);
  for (auto i = size_t{0}; i < group_count; ++i) {
    values[i] = static_cast<int64_t>(distinct_values[i].size());
  }
  return values;
}

// ANY: the first value seen per group, NULL included (The value is passed through. All-NULL groups stay).
template <typename ColumnDataType>
std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> _any_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
    const ColumnID input_column_id) {
  auto seen = std::vector<bool>(group_count, false);
  auto values = ChunkedVector<ColumnDataType>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);

  const auto chunk_count = input_table->chunk_count();
  auto row_index = uint32_t{0};  // global row index across chunks, used to look up the group ticket in `tickets`

  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);

    with_string_segment_iterate<ColumnDataType>(aggregate_segment,
                                                [&](const auto& value, const bool is_null, const auto needs_copy) {
                                                  const auto index = tickets[row_index++];
                                                  if (seen[index]) {
                                                    return;
                                                  }
                                                  seen[index] = true;
                                                  if (is_null) {
                                                    nulls[index] = true;
                                                  } else {
                                                    values[index] = value;
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
std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> _groupby_from_hash_table(
    const GroupKeyData<Concurrent>& group_key_data, const size_t group_count, const size_t groupby_index,
    const size_t string_col_index) {
  const auto& format = group_key_data.row_format;
  const auto& hash_table = group_key_data.global_hash_table;
  auto values = ChunkedVector<ColumnDataType>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);
  const auto null_mask_bit = uint64_t{1} << groupby_index;

  // Reads one group's representative value from its distinct key row into the output slot addressed by its ticket.
  const auto process_entry = [&](const GroupKey& key, const uint64_t ticket) {
    const auto row_view = RowView{key.row, format};

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
    // `ConcurrentTicketMap::for_each` hands the stored key and ticket directly (no `entry` pair). Called after the
    // grouping jobs have joined, so a plain single-threaded pass is safe.
    hash_table.for_each(process_entry);
  } else {
    for (auto it = hash_table.cbegin(); it != hash_table.cend(); ++it) {
      process_entry(it->first, it->second);
    }
  }

  return {std::move(values), std::move(nulls)};
}

// STDDEV: NULL for groups with fewer than two contributing values.
template <typename ColumnDataType>
std::pair<ChunkedVector<double>, ChunkedVector<bool>> _standard_deviation_sample_grouped(
    const uint64_t* const tickets, const size_t group_count, const std::shared_ptr<const Table>& input_table,
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
      // Welford's algorithm tracks its own count in `accumulator[0]`, so the `aggregate_count` argument is unused.
      aggregate_function(std::move(position.value()), size_t{0}, accumulators[tickets[row_index++]]);
    });
  }

  auto values = ChunkedVector<double>(group_count);
  auto nulls = ChunkedVector<bool>(group_count, false);
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
    const auto aggregate_count = _aggregates.size();

    auto column_definitions = TableColumnDefinitions{};
    auto result_values = std::vector<AllTypeVariant>{};
    column_definitions.reserve(aggregate_count);
    result_values.reserve(aggregate_count);

    const auto chunk_count = input_table->chunk_count();
    auto partial_results = std::vector<std::vector<NoGroupbyPartialResult>>(
        chunk_count, std::vector<NoGroupbyPartialResult>(aggregate_count));

    // Compute every aggregate for one chunk. The calling job exclusively owns this chunk's partial-result row.
    const auto compute_chunk_aggregates = [this, &input_table, &partial_results,
                                           aggregate_count](const ChunkID chunk_id) {
      const auto& chunk = input_table->get_chunk(chunk_id);
      for (auto aggregate_id = size_t{0}; aggregate_id < aggregate_count; ++aggregate_id) {
        const auto& aggregate = _aggregates[aggregate_id];
        const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
        const auto input_column_id = pqp_column.column_id;
        if (aggregate->window_function == WindowFunction::Count &&
            (input_column_id == INVALID_COLUMN_ID || !input_table->column_is_nullable(input_column_id))) {
          continue;
        }

        const auto data_type = input_table->column_data_type(input_column_id);
        resolve_data_type(data_type, [&](const auto data_type_t) {
          using ColumnDataType = typename decltype(data_type_t)::type;
          switch (aggregate->window_function) {
            case WindowFunction::Min: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Min>(chunk, input_column_id);
              break;
            }
            case WindowFunction::Max: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Max>(chunk, input_column_id);
              break;
            }
            case WindowFunction::Sum: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Sum>(chunk, input_column_id);
              break;
            }
            case WindowFunction::Avg: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Avg>(chunk, input_column_id);
              break;
            }
            case WindowFunction::Count: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Count>(chunk, input_column_id);
              break;
            }
            case WindowFunction::CountDistinct: {
              partial_results[chunk_id][aggregate_id] = _count_distinct_chunk<ColumnDataType>(chunk, input_column_id);
              break;
            }
            case WindowFunction::StandardDeviationSample:
              if constexpr (std::is_arithmetic_v<ColumnDataType>) {
                partial_results[chunk_id][aggregate_id] =
                    _standard_deviation_chunk<ColumnDataType>(chunk, input_column_id);
              }
              break;
            case WindowFunction::Any: {
              using T = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::ReturnType;
              partial_results[chunk_id][aggregate_id] =
                  _aggregate_chunk<ColumnDataType, T, WindowFunction::Any>(chunk, input_column_id);
              break;
            }
            default:
              Fail("Unsupported aggregate function.");
          }
        });
      }
    };

    if (chunk_count > 0) {
      const auto cpu_count = Hyrise::get().topology.num_cpus();
      const auto worker_count = cpu_count > 1 ? cpu_count - 1 : size_t{1};
      const auto job_count = std::min(worker_count, static_cast<size_t>(chunk_count));
      auto next_chunk_id = std::atomic<uint32_t>{0};
      auto jobs = std::vector<std::shared_ptr<AbstractTask>>{};
      jobs.reserve(job_count);

      for (auto job_id = size_t{0}; job_id < job_count; ++job_id) {
        jobs.emplace_back(std::make_shared<JobTask>([&, chunk_count]() {
          while (true) {
            const auto chunk_id = next_chunk_id.fetch_add(1, std::memory_order_relaxed);
            if (chunk_id >= chunk_count) {
              break;
            }
            compute_chunk_aggregates(ChunkID{chunk_id});
          }
        }));
      }
      Hyrise::get().scheduler()->schedule_and_wait_for_tasks(jobs);
    }

    // Merge the per-chunk partial results into a single result row.
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
                _merge_no_groupby_partials<AggregateType, WindowFunction::Min>(partial_results, aggregate_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Max: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
            const auto [value, is_null] =
                _merge_no_groupby_partials<AggregateType, WindowFunction::Max>(partial_results, aggregate_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Sum: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
            const auto [value, is_null] =
                _merge_no_groupby_partials<AggregateType, WindowFunction::Sum>(partial_results, aggregate_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Avg: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
            const auto [value, is_null] =
                _merge_no_groupby_partials<AggregateType, WindowFunction::Avg>(partial_results, aggregate_id);
            result_values.emplace_back(is_null ? NULL_VALUE : AllTypeVariant{value});
            break;
          }
          case WindowFunction::Count: {
            // Special case for COUNT(*): count all rows, ignoring the input column id.
            if (input_column_id == INVALID_COLUMN_ID || !input_table->column_is_nullable(input_column_id)) {
              auto row_count = input_table->row_count();
              result_values.emplace_back(static_cast<int64_t>(row_count));
              break;
            }
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
            const auto [value, _] =
                _merge_no_groupby_partials<AggregateType, WindowFunction::Count>(partial_results, aggregate_id);
            result_values.emplace_back(value);
            break;
          }
          case WindowFunction::CountDistinct: {
            auto distinct_values = std::unordered_set<AllTypeVariant>{};
            for (auto& chunk_results : partial_results) {
              auto& partial_values = chunk_results[aggregate_id].distinct_values;
              distinct_values.merge(partial_values);
            }
            result_values.emplace_back(static_cast<int64_t>(distinct_values.size()));
            break;
          }
          case WindowFunction::StandardDeviationSample: {
            if constexpr (std::is_arithmetic_v<ColumnDataType>) {
              const auto result = _merge_standard_deviation_partials(partial_results, aggregate_id);
              result_values.emplace_back(result ? AllTypeVariant{*result} : NULL_VALUE);
            } else {
              Fail("StandardDeviationSample is not available on non-arithmetic types.");
            }
            break;
          }
          case WindowFunction::Any: {
            using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::ReturnType;
            const auto [value, is_null] =
                _merge_no_groupby_partials<AggregateType, WindowFunction::Any>(partial_results, aggregate_id);
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
  const auto is_not_immediate_scheduler =
      std::dynamic_pointer_cast<ImmediateExecutionScheduler>(Hyrise::get().scheduler()) == nullptr;
  const auto CONCURRENT = THREAD_COUNT > 1 && is_not_immediate_scheduler;

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

  // Output layout: `output_chunks[chunk][column]`, where the group-by columns occupy the first `groupby_column_count`
  // column slots, followed by one slot per aggregate. Every job produces its column directly as chunk-sized pieces
  // (`ChunkedVector`) and emits them into its fixed column slot of every chunk (`_emit_output_column`), so none of
  // them touch a shared, growing container and the final table assembly is move-only.
  const auto output_chunk_count = (group_count + TARGET_CHUNK_SIZE - 1) / TARGET_CHUNK_SIZE;
  auto output_chunks = std::vector<Segments>(output_chunk_count, Segments(groupby_column_count + aggregate_count));

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
  // input table, and writes into its own `output_chunks` column slot. There are no cross-column dependencies, so we
  // compute one aggregate per job.
  const auto compute_aggregate = [&](const uint32_t aggregate_id) {
    const auto& aggregate = _aggregates[aggregate_id];
    const auto window_function = aggregate->window_function;

    const auto& pqp_column = static_cast<const PQPColumnExpression&>(*aggregate->argument());
    const auto input_column_id = pqp_column.column_id;
    const auto target_index = groupby_column_count + aggregate_id;

    // COUNT(*) does not reference an input column. It counts all rows per group (NULLs included). Every input row
    // contributes its group's ticket exactly once, so the per-group count is just a histogram over the tickets.
    if (window_function == WindowFunction::Count && input_column_id == INVALID_COLUMN_ID) {
      auto values = ChunkedVector<int64_t>(group_count, 0);
      const auto* const tickets = groups->tickets.get();
      const auto row_count = input_table->row_count();
      for (auto row_index = size_t{0}; row_index < row_count; ++row_index) {
        ++values[tickets[row_index]];
      }
      _emit_output_column(std::move(values), {}, false, output_chunks, target_index);
      return;
    }

    resolve_data_type(input_table->column_data_type(input_column_id), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      switch (window_function) {
        case WindowFunction::Min: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Min>(
              groups->tickets.get(), group_count, input_table, input_column_id);
          _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Max>(
              groups->tickets.get(), group_count, input_table, input_column_id);
          _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Sum>(
              groups->tickets.get(), group_count, input_table, input_column_id);
          _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
          break;
        }
        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Avg>(
              groups->tickets.get(), group_count, input_table, input_column_id);
          _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
          break;
        }
        case WindowFunction::Count: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Count>(
              groups->tickets.get(), group_count, input_table, input_column_id);
          // COUNT never produces NULL.
          _emit_output_column(std::move(values), std::move(nulls), false, output_chunks, target_index);
          break;
        }
        case WindowFunction::CountDistinct: {
          auto values =
              _count_distinct_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table, input_column_id);
          _emit_output_column(std::move(values), {}, false, output_chunks, target_index);
          break;
        }
        case WindowFunction::StandardDeviationSample: {
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            auto [values, nulls] = _standard_deviation_sample_grouped<ColumnDataType>(
                groups->tickets.get(), group_count, input_table, input_column_id);
            _emit_output_column(std::move(values), std::move(nulls), true, output_chunks, target_index);
          } else {
            Fail("StandardDeviationSample is not available on non-arithmetic types.");
          }
          break;
        }
        case WindowFunction::Any: {
          auto [values, nulls] =
              _any_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table, input_column_id);
          // ANY() passes the source column through, so the output keeps its nullability.
          _emit_output_column(std::move(values), std::move(nulls), input_table->column_is_nullable(input_column_id),
                              output_chunks, target_index);
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

      auto [values, nulls] = [&]() -> std::pair<ChunkedVector<ColumnDataType>, ChunkedVector<bool>> {
        if (!use_hash_table_for_groupby) {
          // High cardinality: a sequential scan of the source column beats chasing the scattered key rows.
          return _any_grouped<ColumnDataType>(groups->tickets.get(), group_count, input_table, groupby_column_id);
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

      _emit_output_column(std::move(values), std::move(nulls), input_table->column_is_nullable(groupby_column_id),
                          output_chunks, groupby_index);
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

  if (CONCURRENT) {
    auto cleanup_job =
        std::make_shared<JobTask>([groups = std::move(groups), concurrent_groups = std::move(concurrent_groups),
                                   nonconcurrent_groups = std::move(nonconcurrent_groups)]() mutable {
          groups.reset();
          concurrent_groups.reset();
          nonconcurrent_groups.reset();
        });
    cleanup_job->schedule();
  }

  // Every output column was already produced as chunk-sized segments by its own job, so assembling the result table
  // is move-only: no values are copied here.
  auto result_table = std::make_shared<Table>(column_definitions, TableType::Data);
  for (auto& chunk_segments : output_chunks) {
    result_table->append_chunk(std::move(chunk_segments));
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
