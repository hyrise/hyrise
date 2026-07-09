#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class Table;
class OutputColumns;
class StringSpillBuffer;

// ============================================================================================================
// Aggregate value handling for AggregateDYOD (the value side, symmetric to the key schema): both how values are
// SCATTERED (per distinct source column) and how they are ACCUMULATED (per aggregate).
//
// Value NULLs are carried OUT OF BAND, exactly like key NULLs: scatter writes one entry per row in every value stream
// (preserving positional key/value correspondence) and sets a bit in a per-partition value-null-bitmap stream when the
// cell is NULL; the fold skips set bits. There is no in-band NULL sentinel (no full-range value could serve as one).
// ============================================================================================================

// -------- Scatter side: one value stream per DISTINCT source column ----------------------------------------
// A column aggregated by several aggregates is scattered once and shared. COUNT(*) has no source column and no stream.

// One value stream's packing behavior, resolved once per distinct source column at schema build.
class AbstractValueScatterColumn {
 public:
  virtual ~AbstractValueScatterColumn() = default;

  // Bytes this stream writes per row: sizeof(source type) for numeric, or the fixed size of an (offset,length) arena
  // reference for string values (which are stored in the per-partition value arena).
  virtual uint32_t element_width() const = 0;
  virtual bool is_nullable() const = 0;

  // Read the source cell and write its value into value_dest (raw bytes for numeric; an (offset,length) reference into
  // `value_arena` for string, appending the bytes there). If the cell is NULL, set `null_bit_index` in null_bitmap
  // instead (value_dest is then don't-care). `value_arena` is unused for numeric streams.
  virtual void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest,
                    std::byte* null_bitmap, uint32_t null_bit_index, StringSpillBuffer& value_arena) const = 0;
};

// Numeric value stream: writes the source value's raw bytes (no transform -- the accumulator reads them as its native
// type). One instantiation per numeric source type.
template <typename T>
class NumericValueScatterColumn : public AbstractValueScatterColumn {
 public:
  NumericValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;  // sizeof(T)
  bool is_nullable() const override;
  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest, std::byte* null_bitmap,
            uint32_t null_bit_index, StringSpillBuffer& value_arena) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

// String value stream (for MIN/MAX/COUNT on a string column): appends the bytes to the per-partition value arena and
// writes an (offset,length) reference into the fixed-width stream slot. Simplest representation for the first
// iteration -- no inline/hash optimization (values are never hashed or compared as keys).
class StringValueScatterColumn : public AbstractValueScatterColumn {
 public:
  StringValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;  // sizeof(offset,length) reference
  bool is_nullable() const override;
  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest, std::byte* null_bitmap,
            uint32_t null_bit_index, StringSpillBuffer& value_arena) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

// -------- Merge side: one accumulator column per aggregate -------------------------------------------------
// Dense, per-slot accumulator state living alongside the merge map's dense keys (SoA, indexed by dense slot id).
//
// fold is TILE-GRANULAR, not per-row: the one virtual call lands per (aggregate, MERGE_TILE_ROWS tile) and the concrete
// override runs a straight typed loop over the tile, amortizing the dispatch exactly as the reference PoC amortizes its
// match-on-kind. A per-row virtual fold would defeat the design.
class AbstractAccumulatorColumn {
 public:
  virtual ~AbstractAccumulatorColumn() = default;

  // Grow dense state to `slot_count`, seeding new slots with this aggregate's identity. Called by the merge map once
  // per tile after resolve() has created new slots -- never per new key.
  virtual void grow_to(size_t slot_count) = 0;

  // Fold one tile. `slots[i]` is the dense slot for row i; `value_bytes` is this aggregate's value stream over the same
  // tile (empty for COUNT(*), which counts every row); `value_null_bitmap` is the tile's value-null-bitmap (empty when
  // the stream is non-nullable). The override reinterprets `value_bytes` as its input type, skips rows whose null bit
  // is set, and accumulates (bumping the per-slot non-null count where the aggregate needs it).
  virtual void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
                    std::span<const std::byte> value_null_bitmap) = 0;

  // Drop all dense state, retaining capacity, for reuse across the partitions a worker claims.
  virtual void clear() = 0;

  // Append the finalized results for dense slots [first_slot, last_slot) as one contiguous run of output rows into
  // `output`. Applies finalization (AVG divides sum by non-null count; a group with zero non-null contributions emits
  // NULL) and, for string MIN/MAX, appends the accumulated string. Called once per slot-range at flush time.
  virtual void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                             OutputColumns& output) const = 0;
};

// Concrete accumulator column, monomorphized over the input column type and the window function. Instantiated only for
// the (type, function) pairs WindowFunctionTraits marks valid (SUM/AVG on arithmetic; MIN/MAX/COUNT on any type,
// including lexicographic MIN/MAX over strings). AccumulatorType is WindowFunctionTraits<ColumnType,Function>::
// ReturnType, except AVG which carries a running {sum, non-null count} and divides at finalize.
//
// String MIN/MAX hold the running extremum as a SELF-OWNING pmr_string per slot (AccumulatorType == pmr_string): the
// fold decodes the incoming value from the value arena, compares, and copies in a new extremum only when it wins. No
// accumulator-side spill buffer -- extremum changes are infrequent, so the copy is cheap and self-ownership is simplest.
template <typename ColumnType, WindowFunction Function>
class TypedAccumulatorColumn : public AbstractAccumulatorColumn {
 public:
  TypedAccumulatorColumn();

  void grow_to(size_t slot_count) override;
  void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap) override;
  void clear() override;
  void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                     OutputColumns& output) const override;

 private:
  using AccumulatorType = typename WindowFunctionTraits<ColumnType, Function>::ReturnType;

  std::vector<AccumulatorType> _accumulators;  // one per dense slot (a self-owning pmr_string for string MIN/MAX)
  std::vector<uint32_t> _non_null_counts;      // present only when the aggregate needs "seen a non-null value"
};

// Per-query description of the requested aggregates and the value streams they read. Built once from the
// WindowFunctionExpressions (validated against WindowFunctionTraits so invalid combinations such as SUM(string) are
// rejected). Owns the value-scatter columns (one per distinct source column) used by the scatter phase, and builds a
// fresh set of accumulator columns per merge worker (they are mutable per-worker state).
class AggregateSchema {
 public:
  static AggregateSchema build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                               const Table& input_table);

  size_t aggregate_count() const;

  // Result column data type of aggregate `i` (from WindowFunctionTraits), used to build output column definitions.
  DataType result_type(size_t aggregate_index) const;

  // ---- Scatter-phase value-stream model ----
  size_t value_stream_count() const;                         // number of distinct scattered source columns
  const AbstractValueScatterColumn& value_stream(size_t stream_index) const;
  // Value stream aggregate `i` reads, or NO_VALUE_STREAM for COUNT(*).
  static constexpr size_t NO_VALUE_STREAM = ~size_t{0};
  size_t aggregate_value_stream(size_t aggregate_index) const;
  size_t value_null_bitmap_width() const;                    // bytes; 0 if no nullable value stream
  bool needs_value_arena() const;                            // true iff any value stream is a string stream

  // ---- Merge-phase accumulators ----
  // Construct a fresh accumulator column per aggregate for one worker's MergeMap. Dispatches on each entry's
  // (input_type, function) via resolve_data_type -- behavior lives in the TypedAccumulatorColumn classes.
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> make_accumulator_columns() const;

 private:
  static constexpr size_t EXPECTED_AGGREGATE_COLUMNS = 4;

  // Passive per-aggregate configuration resolved at build time; no behavior of its own.
  struct AggregateEntry {
    ColumnID source_column;  // INVALID_COLUMN_ID for COUNT(*)
    WindowFunction function;
    DataType input_type;
    DataType result_type;
    size_t value_stream_index;  // NO_VALUE_STREAM for COUNT(*)
  };

  boost::container::small_vector<AggregateEntry, EXPECTED_AGGREGATE_COLUMNS> _entries;
  boost::container::small_vector<std::unique_ptr<AbstractValueScatterColumn>, EXPECTED_AGGREGATE_COLUMNS> _value_streams;
  uint32_t _value_null_bitmap_width{0};
};

}  // namespace hyrise
