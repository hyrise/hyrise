#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/value_scatter_column.hpp"
#include "resolve_type.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * Per-query description of the requested aggregates and the value streams they read.
 */
class AggregateSchema {
 public:
  /**
   * Build the schema for one query, resolving each aggregate's source column, value stream, and result type.
   */
  static AggregateSchema build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                               const Table& input_table);

  size_t aggregate_count() const;

  DataType result_type(size_t aggregate_index) const;

  WindowFunction function(size_t aggregate_index) const;

  ColumnID source_column(size_t aggregate_index) const;

  /**
   * returns the number of distinct scattered source columns (COUNT(*) contributes none).
   */
  size_t value_stream_count() const;
  const AbstractValueScatterColumn& value_stream(size_t stream_index) const;
  // Sentinel returned by aggregate_value_stream() for an aggregate that scatters no value stream (COUNT(*), ANY).
  static constexpr size_t NO_VALUE_STREAM = ~size_t{0};
  size_t aggregate_value_stream(size_t aggregate_index) const;
  size_t value_null_bitmap_width() const;
  bool needs_value_arena() const;
  bool needs_row_id_stream() const;

  /**
   * Construct a fresh set of accumulator columns (one per aggregate) for a single merge worker's MergeMap.
   *
   * Dispatches on each aggregate's (input_type, function) via resolve_data_type to the matching TypedAccumulatorColumn
   * specialization.
   */
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> make_accumulator_columns() const;

 private:
  // Inline capacity for the per-aggregate small_vectors: most queries request at most this many aggregate columns.
  static constexpr size_t EXPECTED_AGGREGATE_COLUMNS = 4;

  struct AggregateEntry {
    ColumnID source_column;
    WindowFunction function;
    DataType input_type;
    DataType result_type;
    size_t value_stream_index;
  };

  // One AggregateEntry per aggregate, index-aligned with aggregate indices.
  boost::container::small_vector<AggregateEntry, EXPECTED_AGGREGATE_COLUMNS> _entries;
  // Needed by the ANY accumulators to gather representative rows.
  const Table* _input_table{nullptr};
  // One owned scatter column per distinct source column, index-aligned with value-stream indices.
  boost::container::small_vector<std::unique_ptr<AbstractValueScatterColumn>, EXPECTED_AGGREGATE_COLUMNS>
      _value_streams;
  // Cached value_null_bitmap_width() in bytes; 0 when no value stream is nullable.
  uint32_t _value_null_bitmap_width{0};
};

inline DataType resolve_result_type(const DataType input_type, const WindowFunction function) {
  auto result_type = DataType::Null;
  resolve_data_type(input_type, [&](const auto type) {
    using ColumnDataType = typename decltype(type)::type;
    switch (function) {
      case WindowFunction::Min:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Min>::RESULT_TYPE;
        break;
      case WindowFunction::Max:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Max>::RESULT_TYPE;
        break;
      case WindowFunction::Sum:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Sum>::RESULT_TYPE;
        break;
      case WindowFunction::Avg:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Avg>::RESULT_TYPE;
        break;
      case WindowFunction::Count:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Count>::RESULT_TYPE;
        break;
      case WindowFunction::CountDistinct:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::CountDistinct>::RESULT_TYPE;
        break;
      case WindowFunction::Any:
        result_type = WindowFunctionTraits<ColumnDataType, WindowFunction::Any>::RESULT_TYPE;
        break;
      default:
        Fail("Unsupported aggregate function.");
    }
  });
  return result_type;
}

inline AggregateSchema AggregateSchema::build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                                              const Table& input_table) {
  auto schema = AggregateSchema{};
  schema._input_table = &input_table;
  auto nullable_stream_count = size_t{0};

  for (const auto& aggregate : aggregates) {
    const auto function = aggregate->window_function;
    const auto pqp_column = std::dynamic_pointer_cast<PQPColumnExpression>(aggregate->argument());
    Assert(pqp_column, "Aggregates must reference a column.");
    const auto source_column = pqp_column->column_id;

    auto entry = AggregateEntry{};
    entry.source_column = source_column;
    entry.function = function;

    if (source_column == INVALID_COLUMN_ID) {
      Assert(function == WindowFunction::Count, "Only COUNT(*) may aggregate without a source column.");
      entry.input_type = DataType::Null;
      entry.result_type = DataType::Long;
      entry.value_stream_index = NO_VALUE_STREAM;
      schema._entries.emplace_back(entry);
      continue;
    }

    entry.input_type = input_table.column_data_type(source_column);
    entry.result_type = resolve_result_type(entry.input_type, function);
    Assert(entry.result_type != DataType::Null, "Invalid aggregate function for the source column's data type.");

    entry.value_stream_index = NO_VALUE_STREAM;
    if (function == WindowFunction::Any) {
      schema._entries.emplace_back(entry);
      continue;
    }

    for (const auto& earlier : schema._entries) {
      if (earlier.source_column == source_column && earlier.value_stream_index != NO_VALUE_STREAM) {
        entry.value_stream_index = earlier.value_stream_index;
        break;
      }
    }
    if (entry.value_stream_index == NO_VALUE_STREAM) {
      const auto nullable = input_table.column_is_nullable(source_column);
      nullable_stream_count += nullable ? 1 : 0;
      entry.value_stream_index = schema._value_streams.size();
      resolve_data_type(entry.input_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          schema._value_streams.emplace_back(std::make_unique<StringValueScatterColumn>(source_column, nullable));
        } else {
          schema._value_streams.emplace_back(
              std::make_unique<NumericValueScatterColumn<ColumnDataType>>(source_column, nullable));
        }
      });
    }
    schema._entries.emplace_back(entry);
  }

  schema._value_null_bitmap_width = static_cast<uint32_t>((nullable_stream_count + 7) / 8);
  return schema;
}

inline size_t AggregateSchema::aggregate_count() const {
  return _entries.size();
}

inline DataType AggregateSchema::result_type(const size_t aggregate_index) const {
  return _entries[aggregate_index].result_type;
}

inline WindowFunction AggregateSchema::function(const size_t aggregate_index) const {
  return _entries[aggregate_index].function;
}

inline ColumnID AggregateSchema::source_column(const size_t aggregate_index) const {
  return _entries[aggregate_index].source_column;
}

inline size_t AggregateSchema::value_stream_count() const {
  return _value_streams.size();
}

inline const AbstractValueScatterColumn& AggregateSchema::value_stream(const size_t stream_index) const {
  return *_value_streams[stream_index];
}

inline size_t AggregateSchema::aggregate_value_stream(const size_t aggregate_index) const {
  return _entries[aggregate_index].value_stream_index;
}

inline size_t AggregateSchema::value_null_bitmap_width() const {
  return _value_null_bitmap_width;
}

inline bool AggregateSchema::needs_value_arena() const {
  for (const auto& entry : _entries) {
    if (entry.value_stream_index != NO_VALUE_STREAM && entry.input_type == DataType::String) {
      return true;
    }
  }
  return false;
}

inline bool AggregateSchema::needs_row_id_stream() const {
  for (const auto& entry : _entries) {
    if (entry.function == WindowFunction::Any) {
      return true;
    }
  }
  return false;
}

inline std::vector<std::unique_ptr<AbstractAccumulatorColumn>> AggregateSchema::make_accumulator_columns() const {
  auto columns = std::vector<std::unique_ptr<AbstractAccumulatorColumn>>{};
  columns.reserve(_entries.size());

  for (const auto& entry : _entries) {
    if (entry.function == WindowFunction::Any) {
      resolve_data_type(entry.input_type, [&](const auto type) {
        using ColumnDataType = typename decltype(type)::type;
        columns.emplace_back(
            std::make_unique<AnyAccumulatorColumn<ColumnDataType>>(*_input_table, entry.source_column));
      });
      continue;
    }
    if (entry.value_stream_index == NO_VALUE_STREAM) {
      columns.emplace_back(std::make_unique<TypedAccumulatorColumn<int64_t, WindowFunction::Count>>());
      continue;
    }
    resolve_data_type(entry.input_type, [&](const auto type) {
      using ColumnDataType = typename decltype(type)::type;
      switch (entry.function) {
        case WindowFunction::Min:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Min>>());
          break;
        case WindowFunction::Max:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Max>>());
          break;
        case WindowFunction::Count:
          columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Count>>());
          break;
        case WindowFunction::CountDistinct:
          columns.emplace_back(std::make_unique<DistinctAccumulatorColumn<ColumnDataType>>());
          break;
        case WindowFunction::Sum:
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Sum>>());
            break;
          }
          Fail("SUM requires an arithmetic source column.");
        case WindowFunction::Avg:
          if constexpr (std::is_arithmetic_v<ColumnDataType>) {
            columns.emplace_back(std::make_unique<TypedAccumulatorColumn<ColumnDataType, WindowFunction::Avg>>());
            break;
          }
          Fail("AVG requires an arithmetic source column.");
        default:
          Fail("Unsupported aggregate function.");
      }
    });
  }
  return columns;
}

/**
 * Whether a query may take the low-cardinality fast path. COUNT(DISTINCT) needs per-partition value sets and ANY the
 * shared row-id stream, so both stay on the scatter pipeline.
 */
inline bool low_cardinality_eligible(const AggregateSchema& schema) {
  for (auto index = size_t{0}; index < schema.aggregate_count(); ++index) {
    const auto function = schema.function(index);
    if (function == WindowFunction::Any || function == WindowFunction::CountDistinct) {
      return false;
    }
  }
  return true;
}

/**
 * Whether a query's merge phase may spread one partition over several store ranges and combine their maps afterwards.
 * COUNT(DISTINCT) may not: the same value can occur in more than one range, so its per-slot counts cannot be summed.
 */
inline bool merge_split_eligible(const AggregateSchema& schema) {
  for (auto index = size_t{0}; index < schema.aggregate_count(); ++index) {
    if (schema.function(index) == WindowFunction::CountDistinct) {
      return false;
    }
  }
  return true;
}

}  // namespace hyrise
