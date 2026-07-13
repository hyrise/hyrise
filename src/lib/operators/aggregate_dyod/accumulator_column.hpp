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
// Aggregate value handling for AggregateDYOD: the value side of the operator, symmetric to the packed-key KeySchema.
// Covers both how values are scattered (one stream per distinct source column, in the scatter phase) and how they are
// accumulated (one accumulator column per aggregate, in the merge phase).
//
// Value NULLs are carried out of band, exactly like key NULLs: scatter writes one entry per row into every value
// stream (preserving positional key/value correspondence) and, when the cell is NULL, sets a bit in a per-partition
// value-null-bitmap stream; the fold skips set bits. There is no in-band NULL sentinel, because no full-range value
// could reliably serve as one.
//
// Related: AggregateSchema (owns the streams), AbstractAccumulatorColumn (merge-side state), StringSpillBuffer
// (per-partition string arena), OutputColumns (per-worker result sink).
// ============================================================================================================

// -------- Scatter side: one value stream per distinct source column ----------------------------------------
// A column aggregated by several aggregates is scattered once and shared. COUNT(*) has no source column and no stream.

/**
 * One value stream's packing behavior, resolved once per distinct source column at schema build.
 *
 * A value stream reads a source column cell and serializes it into the per-worker scatter store's value lane during
 * the scatter phase, so the merge phase can later fold the raw bytes without touching the input table again. Numeric
 * streams write the value's native bytes; string streams write a (pointer, length) reference into the per-partition
 * value arena. The concrete subclass is chosen per source column from its data type.
 *
 * Invariants: element_width() and is_nullable() are fixed for the stream's lifetime; every pack() call writes exactly
 *   element_width() bytes to value_dest for a non-NULL cell.
 * Ownership/lifetime/threading: owned by AggregateSchema; immutable after build and shared read-only by all scatter
 *   workers, which call pack() concurrently on their own destinations. Must outlive the scatter phase.
 * See also: NumericValueScatterColumn, StringValueScatterColumn; AbstractAccumulatorColumn (merge-side counterpart).
 */
class AbstractValueScatterColumn {
 public:
  virtual ~AbstractValueScatterColumn() = default;

  /**
   * The fixed number of bytes this stream writes to the value lane per row.
   * @return sizeof(source type) for a numeric stream, or the size of a (pointer, length) arena reference for a string
   *   stream (whose payload bytes live in the per-partition value arena, not in the value lane).
   */
  virtual uint32_t element_width() const = 0;
  /** @return true iff the source column is nullable and this stream contributes to the value-null bitmap. */
  virtual bool is_nullable() const = 0;

  /**
   * Read one source cell and serialize it into this worker's scatter store for the merge phase to fold later.
   *
   * @param segment input segment holding the source cell; borrowed, not retained.
   * @param chunk_offset row position of the cell within segment.
   * @param value_dest destination for the packed value; borrowed, must be writable for at least element_width() bytes.
   * @param null_bitmap the value-null bitmap; borrowed, written only when the cell is NULL.
   * @param null_bit_index bit position within null_bitmap for this row.
   * @param value_arena per-partition string arena; string payload bytes are appended here. Unused by numeric streams.
   * @pre runs in the scatter phase, single-threaded per worker on that worker's own destinations.
   * @post for a non-NULL cell, value_dest holds the packed value (native bytes for numeric, a (pointer, length)
   *   reference for string); for a NULL cell, bit null_bit_index of null_bitmap is set and value_dest is unspecified.
   */
  virtual void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest,
                    std::byte* null_bitmap, uint32_t null_bit_index, StringSpillBuffer& value_arena) const = 0;
};

/**
 * Numeric value stream: writes the source value's native bytes with no transform. One instantiation per numeric type.
 *
 * Unlike the key side (where integer lanes are sign-bit-biased and floats canonicalized for correct ordering and
 * equality), values need no ordering, so the bytes are stored verbatim and the accumulator reinterprets them as its
 * native type.
 *
 * Ownership/lifetime/threading: see AbstractValueScatterColumn -- immutable after construction, shared read-only
 *   across scatter workers.
 */
template <typename T>
class NumericValueScatterColumn : public AbstractValueScatterColumn {
 public:
  /**
   * @param source_column input column this stream reads; must index a numeric column of type T.
   * @param nullable whether source_column is nullable (controls value-null-bitmap participation).
   */
  NumericValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;  // sizeof(T)
  bool is_nullable() const override;
  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest, std::byte* null_bitmap,
            uint32_t null_bit_index, StringSpillBuffer& value_arena) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

/**
 * String value stream (for MIN/MAX/COUNT on a string column): appends the value's bytes to the per-partition value
 * arena and writes a (pointer, length) reference into the fixed-width stream slot.
 *
 * This is the simplest workable representation: unlike string keys, string values are never hashed or compared for
 * equality, so there is no inline-prefix or content-hash optimization here, and the reference holds a stable pointer
 * (StringSpillBuffer never relocates live content) rather than an offset that would need arena-base resolution at fold
 * time. element_width() is therefore the size of the (pointer, length) reference, not of the string payload.
 *
 * Ownership/lifetime/threading: see AbstractValueScatterColumn -- immutable after construction, shared read-only
 *   across scatter workers; the referenced payload lives in the value arena passed to pack().
 * See also: TypedAccumulatorColumn for string MIN/MAX, which decodes these references from the arena at fold time.
 */
class StringValueScatterColumn : public AbstractValueScatterColumn {
 public:
  /**
   * @param source_column input column this stream reads; must index a string column.
   * @param nullable whether source_column is nullable (controls value-null-bitmap participation).
   */
  StringValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;  // sizeof(pointer, length) reference
  bool is_nullable() const override;
  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* value_dest, std::byte* null_bitmap,
            uint32_t null_bit_index, StringSpillBuffer& value_arena) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

// -------- Merge side: one accumulator column per aggregate -------------------------------------------------
/**
 * Merge-side accumulator state for one aggregate: dense and per-slot (SoA), indexed by the merge map's dense slot id.
 *
 * During the merge phase a worker streams every scattered row for a claimed partition through its MergeMap: resolve()
 * maps each key to a dense slot, then fold() accumulates the row's value into that slot. State grows to match the
 * map's slot count and is finalized into the worker's OutputColumns at partition-flush time.
 *
 * fold is tile-granular, not per-row: the single virtual call lands per (aggregate, MERGE_TILE_ROWS tile) and the
 * concrete override runs a straight typed loop over the tile, amortizing the dispatch over the tile. A per-row virtual
 * fold would defeat the design.
 *
 * Invariants: dense state has exactly slot_count entries after grow_to(slot_count); slot ids passed to fold() and
 *   finalize_into() are < the current slot count.
 * Ownership/lifetime/threading: mutable per-worker state; each merge worker owns a fresh set (see
 *   AggregateSchema::make_accumulator_columns) and is the sole thread touching it. Not shared across workers.
 * See also: TypedAccumulatorColumn (concrete state), AbstractValueScatterColumn (scatter-side counterpart),
 *   OutputColumns (finalize target).
 */
class AbstractAccumulatorColumn {
 public:
  virtual ~AbstractAccumulatorColumn() = default;

  /**
   * Grow dense state to slot_count entries, seeding each new slot with this aggregate's identity element.
   *
   * @param slot_count new total number of dense slots; must be >= the current slot count (grow-only).
   * @pre called by the MergeMap once per tile, after resolve() created that tile's new slots and before fold() reads
   *   them -- never per new key. Runs single-threaded on the owning worker.
   * @post slots [0, slot_count) are valid; slots that already existed keep their accumulated state.
   * Complexity: O(number of new slots).
   */
  virtual void grow_to(size_t slot_count) = 0;

  /**
   * Fold one tile of rows into their dense slots. One virtual call per tile; a tight typed loop runs inside.
   *
   * The override reinterprets value_bytes as its input type, skips rows whose value-null bit is set, and accumulates
   * into slots[i], bumping the per-slot non-null count where the aggregate needs it.
   *
   * @param slots dense slot id per row in the tile; slots[i] must be < the current slot count. Length is the tile's
   *   row count (<= MERGE_TILE_ROWS).
   * @param value_bytes this aggregate's value stream over the same tile, one element_width()-sized cell per row.
   *   Empty for COUNT(*), which counts every row regardless of value.
   * @param value_null_bitmap the tile's value-null bitmap, one bit per row. Empty when the stream is non-nullable
   *   (no row is NULL). The scatter store keeps per-row bitmap fields (one bit per nullable stream), so the merge
   *   driver gathers this stream-major, bit-per-row form when assembling the tile.
   * @pre grow_to() has already sized dense state to cover every id in slots. Runs single-threaded on the owning worker.
   * @post each row not skipped as NULL is folded into slot slots[i]; per-slot non-null counts reflect contributions
   *   seen so far.
   */
  virtual void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
                    std::span<const std::byte> value_null_bitmap) = 0;

  /**
   * Drop all dense state while retaining allocated capacity, so the column can be reused for the next partition the
   * worker claims (avoids reallocation churn across partitions).
   * @pre runs single-threaded on the owning worker, between partitions.
   * @post slot count is 0; capacity is preserved.
   */
  virtual void clear() = 0;

  /**
   * Append the finalized results for dense slots [first_slot, last_slot) as one contiguous run of output rows.
   *
   * Applies per-aggregate finalization: AVG divides its running sum by the non-null count; a group with zero non-null
   * contributions emits NULL (for SUM/MIN/MAX/AVG alike); string MIN/MAX appends the accumulated extremum string; ANY
   * reads its representative row's cell from the input table, NULL included. Exactly one value (possibly NULL) is
   * appended per slot to output column output_column_index.
   *
   * @param first_slot inclusive start of the dense slot range to emit.
   * @param last_slot exclusive end of the range; must satisfy first_slot <= last_slot <= the current slot count.
   * @param output_column_index index of the output column this aggregate writes; every column receives exactly one
   *   append per emitted row so all columns stay equal-length.
   * @param output the owning worker's thread-local result sink; borrowed and appended to (single writer).
   * @pre every row has been folded for this partition (fold() complete) before any finalize_into() call. Runs
   *   single-threaded on the owning worker.
   * Complexity: O(last_slot - first_slot).
   */
  virtual void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                             OutputColumns& output) const = 0;
};

/**
 * Concrete accumulator column, monomorphized over the input column type and the window function.
 *
 * Instantiated only for the (type, function) pairs WindowFunctionTraits marks valid: SUM/AVG on arithmetic types;
 * MIN/MAX/COUNT on any type, including lexicographic MIN/MAX over strings (ANY lives in AnyAccumulatorColumn
 * instead). AccumulatorType is WindowFunctionTraits<ColumnType, Function>::ReturnType, except AVG, which carries a
 * running {sum, non-null count} and divides at finalize.
 *
 * String MIN/MAX hold the running extremum as a self-owning pmr_string per slot (AccumulatorType == pmr_string): fold
 * decodes the incoming value from the value arena, compares, and copies in a new extremum only when it wins. There is
 * no accumulator-side spill buffer -- extremum changes are infrequent, so the copy is cheap and self-ownership is the
 * simplest option.
 *
 * Invariants: _accumulators.size() is the current dense slot count; _non_null_counts, when used, has the same length.
 * Ownership/lifetime/threading: see AbstractAccumulatorColumn -- one instance per (aggregate, merge worker), mutated
 *   only by its owning worker.
 */
template <typename ColumnType, WindowFunction Function>
class TypedAccumulatorColumn : public AbstractAccumulatorColumn {
 public:
  /** Construct an empty column with zero dense slots; grow_to() sizes it during the merge. */
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

/**
 * Accumulator for ANY: keeps the first row id that lands in each dense slot and gathers the cell from the input table
 * at finalize, NULL included. No value stream is scattered for ANY; fold() consumes the shared row-id stream.
 */
template <typename ColumnType>
class AnyAccumulatorColumn : public AbstractAccumulatorColumn {
 public:
  AnyAccumulatorColumn(const Table& input_table, ColumnID source_column);

  void grow_to(size_t slot_count) override;
  void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap) override;
  void clear() override;
  void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                     OutputColumns& output) const override;

 private:
  const Table* _input_table;
  ColumnID _source_column;
  std::vector<RowID> _row_ids;  // representative row per dense slot; NULL_ROW_ID until the slot's first fold
};

/**
 * Per-query description of the requested aggregates and the value streams they read.
 *
 * Built once from the query's WindowFunctionExpressions and validated against WindowFunctionTraits, so invalid
 * combinations (such as SUM(string), whose result type comes back as DataType::Null) are rejected at build time. It
 * is the single source of truth linking each aggregate to its source column, value stream, and result type. It owns
 * the value-scatter columns (one per distinct source column) read during the scatter phase, and mints a fresh set of
 * accumulator columns per merge worker (mutable per-worker state).
 *
 * Invariants: aggregate indices lie in [0, aggregate_count()); value-stream indices lie in [0, value_stream_count())
 *   or equal NO_VALUE_STREAM for COUNT(*).
 * Ownership/lifetime/threading: one instance per operator execution; immutable after build() and shared read-only by
 *   all scatter and merge workers, so it must outlive both phases. Owns its value-scatter columns; accumulator columns
 *   are handed off to and owned by each worker.
 * See also: AbstractValueScatterColumn, AbstractAccumulatorColumn.
 */
class AggregateSchema {
 public:
  /**
   * Build the schema for one query, resolving each aggregate's source column, value stream, and result type.
   *
   * @param aggregates requested window-function expressions; each is validated against WindowFunctionTraits. Borrowed.
   * @param input_table the operator's input, used to resolve source column data types and nullability. Borrowed.
   * @return a fully resolved schema owning one value-scatter column per distinct source column.
   * @throws std::logic_error (via Hyrise Assert/Fail) if an aggregate is unsupported (only SUM/MIN/MAX/AVG/COUNT/ANY
   *   exist) or its (type, function) combination is invalid -- e.g. SUM(string), whose result type is DataType::Null.
   */
  static AggregateSchema build(const std::vector<std::shared_ptr<WindowFunctionExpression>>& aggregates,
                               const Table& input_table);

  /** @return the number of requested aggregates; valid aggregate indices are [0, aggregate_count()). */
  size_t aggregate_count() const;

  /**
   * @param aggregate_index aggregate to query; must be in [0, aggregate_count()).
   * @return the result column data type of that aggregate (from WindowFunctionTraits), used to build output column
   *   definitions.
   */
  DataType result_type(size_t aggregate_index) const;

  /**
   * @param aggregate_index aggregate to query; must be in [0, aggregate_count()).
   * @return that aggregate's window function.
   */
  WindowFunction function(size_t aggregate_index) const;

  /**
   * @param aggregate_index aggregate to query; must be in [0, aggregate_count()).
   * @return that aggregate's source column, or INVALID_COLUMN_ID for COUNT(*).
   */
  ColumnID source_column(size_t aggregate_index) const;

  // ---- Scatter-phase value-stream model ----
  /** @return the number of distinct scattered source columns (COUNT(*) contributes none). */
  size_t value_stream_count() const;
  /**
   * @param stream_index which value stream; must be in [0, value_stream_count()).
   * @return the scatter column describing that stream; borrowed, valid while this schema lives.
   */
  const AbstractValueScatterColumn& value_stream(size_t stream_index) const;
  // Sentinel returned by aggregate_value_stream() for an aggregate that scatters no value stream (COUNT(*), ANY).
  static constexpr size_t NO_VALUE_STREAM = ~size_t{0};
  /**
   * @param aggregate_index aggregate to query; must be in [0, aggregate_count()).
   * @return the value-stream index (in [0, value_stream_count())) this aggregate reads, or NO_VALUE_STREAM if the
   *   aggregate scatters no value stream: COUNT(*), which has no source column, and ANY, which reads the shared
   *   row-id stream instead.
   */
  size_t aggregate_value_stream(size_t aggregate_index) const;
  /** @return the value-null-bitmap width in bytes; 0 when no value stream is nullable. */
  size_t value_null_bitmap_width() const;
  /** @return true iff any value stream is a string stream and a per-partition value arena must be allocated. */
  bool needs_value_arena() const;
  /** @return true iff an ANY aggregate is present and the scatter phase must emit the shared row-id stream. */
  bool needs_row_id_stream() const;

  // ---- Merge-phase accumulators ----
  /**
   * Construct a fresh set of accumulator columns (one per aggregate) for a single merge worker's MergeMap.
   *
   * Dispatches on each aggregate's (input_type, function) via resolve_data_type to the matching TypedAccumulatorColumn
   * specialization, where the behavior lives.
   *
   * @return one owning accumulator column per aggregate, index-aligned with aggregate indices, each empty until the
   *   worker sizes it via grow_to().
   * @pre call once per merge worker; the returned columns are mutable per-worker state and must not be shared.
   */
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> make_accumulator_columns() const;

 private:
  // Inline capacity for the per-aggregate small_vectors: most queries request at most this many aggregate columns.
  static constexpr size_t EXPECTED_AGGREGATE_COLUMNS = 4;

  // Passive per-aggregate configuration resolved at build time; no behavior of its own.
  struct AggregateEntry {
    ColumnID source_column;     // source column to aggregate; INVALID_COLUMN_ID for COUNT(*)
    WindowFunction function;    // requested aggregate function (SUM/MIN/MAX/AVG/COUNT/ANY)
    DataType input_type;        // data type of source_column, the value folded into the accumulator
    DataType result_type;       // output data type of this aggregate, from WindowFunctionTraits
    size_t value_stream_index;  // index into the value streams, or NO_VALUE_STREAM for COUNT(*) and ANY
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

}  // namespace hyrise
