#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/value_scatter_column.hpp"
#include "storage/table.hpp"
#include "types.hpp"

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
    WindowFunction function{WindowFunction::Count};
    DataType input_type{DataType::Null};
    DataType result_type{DataType::Null};
    size_t value_stream_index{NO_VALUE_STREAM};
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

/**
 * Resolve an aggregate's result type from its source column's data type and window function.
 */
DataType resolve_result_type(DataType input_type, WindowFunction function);

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

/**
 * Whether a query may take the low-cardinality fast path. COUNT(DISTINCT) needs per-partition value sets and ANY the
 * shared row-id stream, so both stay on the scatter pipeline.
 */
bool low_cardinality_eligible(const AggregateSchema& schema);

/**
 * Whether a query's merge phase may spread one partition over several store ranges and combine their maps afterwards.
 * COUNT(DISTINCT) may not: the same value can occur in more than one range, so its per-slot counts cannot be summed.
 */
bool merge_split_eligible(const AggregateSchema& schema);

}  // namespace hyrise
