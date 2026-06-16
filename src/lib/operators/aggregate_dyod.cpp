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

// Computes a single aggregate over the whole table (no group-by). Returns the value and whether it is NULL: an
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

// Hash for a group key (the concatenated group-by column values of a row). There is no std::hash for
// std::vector<AllTypeVariant>, so we combine the per-value hashes ourselves.
struct GroupKeyHash {
  size_t operator()(const std::vector<AllTypeVariant>& key) const {
    auto seed = size_t{0};
    for (const auto& value : key) {
      boost::hash_combine(seed, std::hash<AllTypeVariant>{}(value));
    }
    return seed;
  }
};

// Equality for group keys. `AllTypeVariant`'s own `==` follows SQL ternary logic (NULL compares unequal to everything,
// including NULL), but for grouping all NULLs must fall into a single group. We therefore treat two NULLs as equal.
struct GroupKeyEqual {
  bool operator()(const std::vector<AllTypeVariant>& lhs, const std::vector<AllTypeVariant>& rhs) const {
    const auto size = lhs.size();
    if (size != rhs.size()) {
      return false;
    }
    for (auto i = size_t{0}; i < size; ++i) {
      const auto lhs_null = variant_is_null(lhs[i]);
      const auto rhs_null = variant_is_null(rhs[i]);
      if (lhs_null || rhs_null) {
        if (lhs_null != rhs_null) {
          return false;
        }
        // Both NULL: equal, keep comparing the remaining columns.
      } else if (!(lhs[i] == rhs[i])) {
        return false;
      }
    }
    return true;
  }
};

// STDDEV_SAMP needs a richer accumulator than the other aggregates (Welford's running mean/variance, see
// `WindowFunctionBuilder<..., WindowFunction::StandardDeviationSample>`), so it gets its own scan helpers instead of
// reusing `_aggregate_all_values`. The result is NULL whenever fewer than two values contribute, which
// the callers turn into a NULL output value.
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

// The shared skeleton for the group-by path. All output columns (the group-by columns and every aggregate) are
// indexed by a group's position in `keys`, so they line up row-for-row regardless of hash-map iteration order.
using GroupKey = std::vector<AllTypeVariant>;

struct GroupKeyData {
  std::vector<GroupKey> keys;  // distinct group keys, first-encounter order
  std::unordered_map<GroupKey, size_t, GroupKeyHash, GroupKeyEqual> index;  // group key -> its position in `keys`
  std::vector<size_t> row_counts;                                           // number of rows per group (for COUNT(*))
};

// Reads the group-by column values of `chunk_offset` into `key`.
inline void _read_group_key(const std::shared_ptr<const Chunk>& chunk, const std::vector<ColumnID>& groupby_column_ids,
                            const ChunkOffset chunk_offset, GroupKey& key) {
  const auto group_by_column_count = groupby_column_ids.size();
  for (auto group_index = size_t{0}; group_index < group_by_column_count; ++group_index) {
    key[group_index] = chunk->get_segment(groupby_column_ids[group_index])->operator[](chunk_offset);
  }
}

// Determines the distinct groups. A group exists if any row maps to it, even if its aggregated values are all NULL or
// its group-by key contains NULL (NULL forms its own group).
GroupKeyData _compute_group_keys(const std::vector<ColumnID>& groupby_column_ids,
                                 const std::shared_ptr<const Table>& input_table) {
  auto data = GroupKeyData{};
  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();
  auto key = GroupKey(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto chunk_size = chunk->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
      _read_group_key(chunk, groupby_column_ids, chunk_offset, key);
      const auto [iter, inserted] = data.index.try_emplace(key, data.keys.size());
      if (inserted) {
        data.keys.push_back(key);
        data.row_counts.push_back(0);
      }
      ++data.row_counts[iter->second];
    }
  }
  return data;
}

// Builds the group-by output segments (one per group-by column) from the precomputed group keys, preserving the source
// column's nullability (a NULL group-by key value is emitted as NULL).
void _build_groupby_segments(const GroupKeyData& groups, const std::vector<ColumnID>& groupby_column_ids,
                             const std::shared_ptr<const Table>& input_table,
                             pmr_vector<std::shared_ptr<AbstractSegment>>& output_segments) {
  const auto group_count = groups.keys.size();
  const auto group_by_column_count = groupby_column_ids.size();
  for (auto group_index = size_t{0}; group_index < group_by_column_count; ++group_index) {
    const auto column_id = groupby_column_ids[group_index];
    const auto is_nullable = input_table->column_is_nullable(column_id);
    resolve_data_type(input_table->column_data_type(column_id), [&](const auto column_data_type_t) {
      using GroupKeyDataType = typename decltype(column_data_type_t)::type;
      auto values = pmr_vector<GroupKeyDataType>(group_count);
      auto nulls = pmr_vector<bool>(group_count, false);
      for (auto i = size_t{0}; i < group_count; ++i) {
        const auto& key_value = groups.keys[i][group_index];
        if (variant_is_null(key_value)) {
          nulls[i] = true;
        } else {
          values[i] = boost::get<GroupKeyDataType>(key_value);
        }
      }
      if (is_nullable) {
        output_segments.emplace_back(
            std::make_shared<ValueSegment<GroupKeyDataType>>(std::move(values), std::move(nulls)));
      } else {
        output_segments.emplace_back(std::make_shared<ValueSegment<GroupKeyDataType>>(std::move(values)));
      }
    });
  }
}

// Incrementally computable aggregates (MIN/MAX/SUM/AVG/COUNT), indexed per group. A group with no contributing
// (non-NULL) value yields NULL, except COUNT which yields 0.
template <typename ColumnDataType, typename AggregateType, WindowFunction window_function>
std::pair<pmr_vector<AggregateType>, pmr_vector<bool>> _aggregate_grouped(
    const GroupKeyData& groups, const std::vector<ColumnID>& groupby_column_ids,
    const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, AggregateType, window_function>().get_aggregate_function();
  const auto group_count = groups.keys.size();
  auto accumulators = std::vector<AggregateType>(group_count);
  auto value_counts = std::vector<size_t>(group_count, 0);

  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();
  auto key = GroupKey(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    const auto segment_size = aggregate_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = aggregate_segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        continue;
      }
      _read_group_key(chunk, groupby_column_ids, chunk_offset, key);
      const auto index = groups.index.at(key);
      aggregate_function(boost::get<ColumnDataType>(value), value_counts[index], accumulators[index]);
      ++value_counts[index];
    }
  }

  auto values = pmr_vector<AggregateType>(group_count);
  auto nulls = pmr_vector<bool>(group_count, false);
  for (auto i = size_t{0}; i < group_count; ++i) {
    if constexpr (window_function == WindowFunction::Count) {
      values[i] = static_cast<AggregateType>(value_counts[i]);
    } else if constexpr (window_function == WindowFunction::Avg && std::is_arithmetic_v<AggregateType>) {
      if (value_counts[i] == 0) {
        nulls[i] = true;
      } else {
        values[i] = accumulators[i] / static_cast<AggregateType>(value_counts[i]);
      }
    } else {
      // MIN/MAX/SUM are NULL when no non-NULL value contributed.
      if (value_counts[i] == 0) {
        nulls[i] = true;
      } else {
        values[i] = accumulators[i];
      }
    }
  }
  return {std::move(values), std::move(nulls)};
}

// COUNT(DISTINCT), indexed per group: number of distinct non-NULL values. Never NULL (0 for an all-NULL group).
template <typename ColumnDataType>
pmr_vector<int64_t> _count_distinct_grouped(const GroupKeyData& groups, const std::vector<ColumnID>& groupby_column_ids,
                                            const std::shared_ptr<const Table>& input_table,
                                            const ColumnID input_column_id) {
  const auto group_count = groups.keys.size();
  auto distinct_values = std::vector<std::unordered_set<ColumnDataType>>(group_count);

  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();
  auto key = GroupKey(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    const auto segment_size = aggregate_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = aggregate_segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        continue;
      }
      _read_group_key(chunk, groupby_column_ids, chunk_offset, key);
      distinct_values[groups.index.at(key)].insert(boost::get<ColumnDataType>(value));
    }
  }

  auto values = pmr_vector<int64_t>(group_count);
  for (auto i = size_t{0}; i < group_count; ++i) {
    values[i] = static_cast<int64_t>(distinct_values[i].size());
  }
  return values;
}

// ANY(), indexed per group: the first value seen per group, NULL included (the value is passed through, not
// aggregated, so all-NULL groups stay).
template <typename ColumnDataType>
std::pair<pmr_vector<ColumnDataType>, pmr_vector<bool>> _any_grouped(const GroupKeyData& groups,
                                                                     const std::vector<ColumnID>& groupby_column_ids,
                                                                     const std::shared_ptr<const Table>& input_table,
                                                                     const ColumnID input_column_id) {
  const auto group_count = groups.keys.size();
  auto seen = std::vector<bool>(group_count, false);
  auto values = pmr_vector<ColumnDataType>(group_count);
  auto nulls = pmr_vector<bool>(group_count, false);

  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();
  auto key = GroupKey(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    const auto segment_size = aggregate_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      _read_group_key(chunk, groupby_column_ids, chunk_offset, key);
      const auto index = groups.index.at(key);
      if (seen[index]) {
        continue;
      }
      seen[index] = true;
      const auto value = aggregate_segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        nulls[index] = true;
      } else {
        values[index] = boost::get<ColumnDataType>(value);
      }
    }
  }
  return {std::move(values), std::move(nulls)};
}

// STDDEV_SAMP, indexed per group: NULL for groups with fewer than two contributing values.
template <typename ColumnDataType>
std::pair<pmr_vector<double>, pmr_vector<bool>> _standard_deviation_sample_grouped(
    const GroupKeyData& groups, const std::vector<ColumnID>& groupby_column_ids,
    const std::shared_ptr<const Table>& input_table, const ColumnID input_column_id) {
  static_assert(std::is_arithmetic_v<ColumnDataType>, "StandardDeviationSample is only defined on arithmetic types.");
  const auto aggregate_function =
      WindowFunctionBuilder<ColumnDataType, double, WindowFunction::StandardDeviationSample>().get_aggregate_function();
  const auto group_count = groups.keys.size();
  auto accumulators = std::vector<StandardDeviationSampleData>(group_count);

  const auto group_by_column_count = groupby_column_ids.size();
  const auto chunk_count = input_table->chunk_count();
  auto key = GroupKey(group_by_column_count);
  for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    const auto& chunk = input_table->get_chunk(chunk_id);
    const auto& aggregate_segment = chunk->get_segment(input_column_id);
    const auto segment_size = aggregate_segment->size();
    for (auto chunk_offset = ChunkOffset{0}; chunk_offset < segment_size; ++chunk_offset) {
      const auto value = aggregate_segment->operator[](chunk_offset);
      if (variant_is_null(value)) {
        continue;
      }
      _read_group_key(chunk, groupby_column_ids, chunk_offset, key);
      // Welford's algorithm tracks its own count in `accumulator[0]`, so the `aggregate_count` argument is unused.
      aggregate_function(boost::get<ColumnDataType>(value), size_t{0}, accumulators[groups.index.at(key)]);
    }
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
    const auto chunk_count = input_table->chunk_count();

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

  const auto groups = _compute_group_keys(_groupby_column_ids, input_table);
  const auto group_count = groups.keys.size();

  auto column_definitions = TableColumnDefinitions{};
  column_definitions.reserve(groupby_column_count + aggregate_count);
  auto output_segments = pmr_vector<std::shared_ptr<AbstractSegment>>{};
  output_segments.reserve(groupby_column_count + aggregate_count);

  // The output schema is [group-by columns..., aggregate columns...].
  for (const auto groupby_column_id : _groupby_column_ids) {
    column_definitions.emplace_back(input_table->column_name(groupby_column_id),
                                    input_table->column_data_type(groupby_column_id),
                                    input_table->column_is_nullable(groupby_column_id));
  }
  _build_groupby_segments(groups, _groupby_column_ids, input_table, output_segments);

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
    } else {
      // COUNT and COUNT DISTINCT never produce NULL; all other aggregates can.
      const auto is_count =
          window_function == WindowFunction::Count || window_function == WindowFunction::CountDistinct;
      column_definitions.emplace_back(aggregate->as_column_name(), aggregate->data_type(), !is_count);
    }

    // COUNT(*) does not reference an input column; it counts all rows per group (NULLs included).
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
              groups, _groupby_column_ids, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Max: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Max>(
              groups, _groupby_column_ids, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Sum: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Sum>(
              groups, _groupby_column_ids, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Avg: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::ReturnType;
          auto [values, nulls] = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Avg>(
              groups, _groupby_column_ids, input_table, input_column_id);
          output_segments.emplace_back(
              std::make_shared<ValueSegment<AggregateType>>(std::move(values), std::move(nulls)));
          break;
        }
        case WindowFunction::Count: {
          using AggregateType = typename WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::ReturnType;
          auto result = _aggregate_grouped<ColumnDataType, AggregateType, WindowFunction::Count>(
              groups, _groupby_column_ids, input_table, input_column_id);
          // COUNT never produces NULL.
          output_segments.emplace_back(std::make_shared<ValueSegment<AggregateType>>(std::move(result.first)));
          break;
        }
        case WindowFunction::CountDistinct: {
          auto values =
              _count_distinct_grouped<ColumnDataType>(groups, _groupby_column_ids, input_table, input_column_id);
          output_segments.emplace_back(std::make_shared<ValueSegment<int64_t>>(std::move(values)));
          break;
        }
        case WindowFunction::StandardDeviationSample: {
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            auto [values, nulls] = _standard_deviation_sample_grouped<ColumnDataType>(groups, _groupby_column_ids,
                                                                                      input_table, input_column_id);
            output_segments.emplace_back(std::make_shared<ValueSegment<double>>(std::move(values), std::move(nulls)));
          } else {
            Fail("StandardDeviationSample is not available on non-arithmetic types.");
          }
          break;
        }
        case WindowFunction::Any: {
          auto [values, nulls] =
              _any_grouped<ColumnDataType>(groups, _groupby_column_ids, input_table, input_column_id);
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
  // An empty input produces no groups; a 0-row chunk must not be appended (see `Table::append_chunk`).
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
