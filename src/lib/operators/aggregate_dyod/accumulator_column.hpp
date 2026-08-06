#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "all_type_variant.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/window_function_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/distinct_set.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Fixed-width cell a string value stream writes: a pointer into the per-partition value arena plus the length.
struct StringValueReference {
  const std::byte* data;
  uint64_t length;
};

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
 * A value stream reads a source column and serializes it into the per-worker scatter store's value lane during the
 * scatter phase, so the merge phase can later fold the raw bytes without touching the input table again. Numeric
 * streams write the value's native bytes; string streams write a (pointer, length) reference into the per-partition
 * value arena. The concrete subclass is chosen per source column from its data type.
 *
 * Invariants: element_width() and is_nullable() are fixed for the stream's lifetime; scatter() writes exactly
 *   element_width() bytes to the stream's value lane per row.
 * Ownership/lifetime/threading: owned by AggregateSchema; immutable after build and shared read-only by all scatter
 *   workers, which call scatter() concurrently on their own destinations. Must outlive the scatter phase.
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
   * Scatter one chunk's source column into the worker's store, one typed segment_iterate pass over the whole column.
   *
   * NULL cells push a zeroed value and set their bit in the row's field of the chunk's value-null-bitmap scratch,
   * which the caller pushes row-wise after all value streams ran.
   *
   * @param segment input segment holding the source column for the current chunk; borrowed, not retained.
   * @param row_partitions destination partition per chunk row, computed by the key pass; borrowed.
   * @param stream the ScatterHeads stream index of this value stream.
   * @param heads the worker's SWWC staging front-end; borrowed, mutated.
   * @param store the worker's scatter store; borrowed, mutated. String streams append payload bytes to the
   *   destination partition's value arena.
   * @param null_bitmap the chunk's value-null-bitmap scratch, null_bitmap_width bytes per row; written only for NULL
   *   cells. May be null when no value stream is nullable.
   * @param null_bitmap_width per-row width of the bitmap scratch in bytes.
   * @param null_bit_index bit position of this stream within a row's bitmap field.
   * @pre runs in the scatter phase, single-threaded per worker on that worker's own store and scratch.
   */
  virtual void scatter(const AbstractSegment& segment, std::span<const PartitionId> row_partitions, size_t stream,
                       ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap, size_t null_bitmap_width,
                       uint32_t null_bit_index) const = 0;
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
  void scatter(const AbstractSegment& segment, std::span<const PartitionId> row_partitions, size_t stream,
               ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap, size_t null_bitmap_width,
               uint32_t null_bit_index) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

template <typename T>
NumericValueScatterColumn<T>::NumericValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {}

template <typename T>
uint32_t NumericValueScatterColumn<T>::element_width() const {
  return sizeof(T);
}

template <typename T>
bool NumericValueScatterColumn<T>::is_nullable() const {
  return _nullable;
}

template <typename T>
void NumericValueScatterColumn<T>::scatter(const AbstractSegment& segment,
                                           const std::span<const PartitionId> row_partitions, const size_t stream,
                                           ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap,
                                           const size_t null_bitmap_width, const uint32_t null_bit_index) const {
  auto row = size_t{0};
  segment_iterate<T>(segment, [&](const auto& position) {
    const auto partition = row_partitions[row];
    auto value = T{};
    if (position.is_null()) {
      DebugAssert(_nullable, "NULL in a non-nullable value column.");
      set_null_bit(null_bitmap + row * null_bitmap_width, null_bit_index);
    } else {
      value = position.value();
    }
    heads.push(store, stream, partition, reinterpret_cast<const std::byte*>(&value), sizeof(value));
    ++row;
  });
}

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
 *   across scatter workers; the referenced payload lives in the per-partition value arena.
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
  void scatter(const AbstractSegment& segment, std::span<const PartitionId> row_partitions, size_t stream,
               ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap, size_t null_bitmap_width,
               uint32_t null_bit_index) const override;

 private:
  ColumnID _source_column;
  bool _nullable;
};

inline StringValueScatterColumn::StringValueScatterColumn(const ColumnID source_column, const bool nullable)
    : _source_column{source_column}, _nullable{nullable} {}

inline uint32_t StringValueScatterColumn::element_width() const {
  return sizeof(StringValueReference);
}

inline bool StringValueScatterColumn::is_nullable() const {
  return _nullable;
}

inline void StringValueScatterColumn::scatter(const AbstractSegment& segment,
                                              const std::span<const PartitionId> row_partitions, const size_t stream,
                                              ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap,
                                              const size_t null_bitmap_width, const uint32_t null_bit_index) const {
  auto row = size_t{0};
  segment_iterate<pmr_string>(segment, [&](const auto& position) {
    const auto partition = row_partitions[row];
    auto reference = StringValueReference{};
    if (position.is_null()) {
      DebugAssert(_nullable, "NULL in a non-nullable value column.");
      set_null_bit(null_bitmap + row * null_bitmap_width, null_bit_index);
    } else {
      const auto& value = position.value();
      reference.data =
          store.value_arena(partition).append(reinterpret_cast<const std::byte*>(value.data()), value.size());
      reference.length = value.size();
    }
    heads.push(store, stream, partition, reinterpret_cast<const std::byte*>(&reference), sizeof(reference));
    ++row;
  });
}

// -------- Merge side: one accumulator column per aggregate -------------------------------------------------
/**
 * Merge-side accumulator state for one aggregate: dense and per-slot (SoA), indexed by the merge map's dense slot id.
 *
 * During the merge phase a worker streams every scattered row for a claimed partition through its MergeMap: resolve()
 * maps each key to a dense slot, then fold() accumulates the row's value into that slot. State grows to match the
 * map's slot count and is finalized into the worker's OutputColumns at partition-flush time.
 *
 * fold is tile-granular, not per-row: the single virtual call lands per (aggregate, merge_tile_rows() tile) and the
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
   *   row count (<= merge_tile_rows()).
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
   * Merge another accumulator column's per-slot state into this one, used by the low-cardinality path to combine
   * per-worker private maps. For each row i, the source slot (other_first_slot + i) of `other` is folded into this
   * column's dense slot destination_slots[i].
   */
  virtual void combine_from(const AbstractAccumulatorColumn& other, size_t other_first_slot,
                            std::span<const uint32_t> destination_slots) = 0;

  /**
   * Append the finalized results for dense slots [first_slot, last_slot) as one contiguous run of output rows.
   *
   * Applies per-aggregate finalization: AVG divides its running sum by the non-null count; a group with zero non-null
   * contributions emits NULL (for SUM/MIN/MAX/AVG alike, while COUNT and COUNT(DISTINCT) emit 0); string MIN/MAX
   * appends the accumulated extremum string; ANY reads its representative row's cell from the input table, NULL
   * included. Exactly one value (possibly NULL) is appended per slot to output column output_column_index.
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
 * MIN/MAX/COUNT on any type, including lexicographic MIN/MAX over strings (ANY and COUNT(DISTINCT) live in
 * AnyAccumulatorColumn and DistinctAccumulatorColumn instead). AccumulatorType is
 * WindowFunctionTraits<ColumnType, Function>::ReturnType, except AVG, which carries a running {sum, non-null count}
 * and divides at finalize.
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
  void combine_from(const AbstractAccumulatorColumn& other, size_t other_first_slot,
                    std::span<const uint32_t> destination_slots) override;
  void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                     OutputColumns& output) const override;

 private:
  using AccumulatorType = typename WindowFunctionTraits<ColumnType, Function>::ReturnType;

  std::vector<AccumulatorType> _accumulators;  // one per dense slot (a self-owning pmr_string for string MIN/MAX)
  std::vector<uint32_t> _non_null_counts;      // present only when the aggregate needs "seen a non-null value"
};

template <typename ColumnType, WindowFunction Function>
TypedAccumulatorColumn<ColumnType, Function>::TypedAccumulatorColumn() = default;

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::grow_to(const size_t slot_count) {
  DebugAssert(slot_count >= _accumulators.size(), "Dense accumulator state only grows.");
  _accumulators.resize(slot_count);
  if constexpr (Function != WindowFunction::Count) {
    _non_null_counts.resize(slot_count, 0);
  }
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::fold(std::span<const uint32_t> slots,
                                                        std::span<const std::byte> value_bytes,
                                                        std::span<const std::byte> value_null_bitmap) {
  const auto row_count = slots.size();
  DebugAssert(value_null_bitmap.empty() || value_null_bitmap.size() * 8 >= row_count,
              "Value-null bitmap does not cover the tile.");
  if constexpr (Function != WindowFunction::Count) {
    [[maybe_unused]] constexpr auto VALUE_WIDTH =
        std::is_same_v<ColumnType, pmr_string> ? sizeof(StringValueReference) : sizeof(ColumnType);
    DebugAssert(value_bytes.size() == row_count * VALUE_WIDTH, "Value tile does not match the slot tile.");
  }

  if constexpr (Function == WindowFunction::Count) {
    if (value_bytes.empty()) {
      for (auto row = size_t{0}; row < row_count; ++row) {
        ++_accumulators[slots[row]];
      }
      return;
    }
  }

  for (auto row = size_t{0}; row < row_count; ++row) {
    if (!value_null_bitmap.empty() && null_bit_set(value_null_bitmap.data(), row)) {
      continue;
    }
    const auto slot = slots[row];
    if constexpr (Function == WindowFunction::Count) {
      ++_accumulators[slot];
    } else if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      auto reference = StringValueReference{};
      std::memcpy(&reference, value_bytes.data() + row * sizeof(reference), sizeof(reference));
      const auto value = std::string_view{reinterpret_cast<const char*>(reference.data), reference.length};
      auto& count = _non_null_counts[slot];
      auto& current = _accumulators[slot];
      if constexpr (Function == WindowFunction::Min) {
        if (count == 0 || value < current) {
          current = pmr_string{value};
        }
      } else if constexpr (Function == WindowFunction::Max) {
        if (count == 0 || value > current) {
          current = pmr_string{value};
        }
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    } else {
      auto value = ColumnType{};
      std::memcpy(&value, value_bytes.data() + row * sizeof(value), sizeof(value));
      auto& count = _non_null_counts[slot];
      if constexpr (Function == WindowFunction::Min) {
        if (count == 0 || value < _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (Function == WindowFunction::Max) {
        if (count == 0 || value > _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (Function == WindowFunction::Sum || Function == WindowFunction::Avg) {
        _accumulators[slot] += static_cast<AccumulatorType>(value);
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    }
  }
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::clear() {
  _accumulators.clear();
  _non_null_counts.clear();
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::combine_from(const AbstractAccumulatorColumn& other_base,
                                                                const size_t other_first_slot,
                                                                const std::span<const uint32_t> destination_slots) {
  const auto& other = static_cast<const TypedAccumulatorColumn&>(other_base);
  const auto row_count = destination_slots.size();
  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto source_slot = other_first_slot + row;
    const auto slot = destination_slots[row];
    if constexpr (Function == WindowFunction::Count) {
      _accumulators[slot] += other._accumulators[source_slot];
    } else if constexpr (Function == WindowFunction::Sum || Function == WindowFunction::Avg) {
      _accumulators[slot] += other._accumulators[source_slot];
      _non_null_counts[slot] += other._non_null_counts[source_slot];
    } else if constexpr (Function == WindowFunction::Min || Function == WindowFunction::Max) {
      const auto source_count = other._non_null_counts[source_slot];
      if (source_count > 0) {
        if (_non_null_counts[slot] == 0) {
          _accumulators[slot] = other._accumulators[source_slot];
        } else if constexpr (Function == WindowFunction::Min) {
          if (other._accumulators[source_slot] < _accumulators[slot]) {
            _accumulators[slot] = other._accumulators[source_slot];
          }
        } else {
          if (other._accumulators[source_slot] > _accumulators[slot]) {
            _accumulators[slot] = other._accumulators[source_slot];
          }
        }
        _non_null_counts[slot] += source_count;
      }
    } else {
      Fail("Unsupported aggregate function.");
    }
  }
}

template <typename ColumnType, WindowFunction Function>
void TypedAccumulatorColumn<ColumnType, Function>::finalize_into(const size_t first_slot, const size_t last_slot,
                                                                 const size_t output_column_index,
                                                                 OutputColumns& output) const {
  constexpr auto IS_MIN_MAX_OR_SUM =
      Function == WindowFunction::Min || Function == WindowFunction::Max || Function == WindowFunction::Sum;
  auto& output_column = static_cast<TypedOutputColumn<AccumulatorType>&>(output.column(output_column_index));
  for (auto slot = first_slot; slot < last_slot; ++slot) {
    if constexpr (Function == WindowFunction::Count) {
      output_column.append(_accumulators[slot]);
    } else if constexpr (Function == WindowFunction::Avg) {
      if (_non_null_counts[slot] == 0) {
        output_column.append_null();
      } else {
        output_column.append(_accumulators[slot] / static_cast<AccumulatorType>(_non_null_counts[slot]));
      }
    } else if constexpr (IS_MIN_MAX_OR_SUM) {
      if (_non_null_counts[slot] == 0) {
        output_column.append_null();
      } else {
        output_column.append(_accumulators[slot]);
      }
    } else {
      Fail("Unsupported aggregate function.");
    }
  }
}

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
  void combine_from(const AbstractAccumulatorColumn& other, size_t other_first_slot,
                    std::span<const uint32_t> destination_slots) override;
  void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                     OutputColumns& output) const override;

 private:
  const Table* _input_table;
  ColumnID _source_column;
  std::vector<RowID> _row_ids;  // representative row per dense slot; NULL_ROW_ID until the slot's first fold
};

template <typename ColumnType>
AnyAccumulatorColumn<ColumnType>::AnyAccumulatorColumn(const Table& input_table, const ColumnID source_column)
    : _input_table{&input_table}, _source_column{source_column} {}

template <typename ColumnType>
void AnyAccumulatorColumn<ColumnType>::grow_to(const size_t slot_count) {
  DebugAssert(slot_count >= _row_ids.size(), "Dense accumulator state only grows.");
  _row_ids.resize(slot_count, NULL_ROW_ID);
}

template <typename ColumnType>
void AnyAccumulatorColumn<ColumnType>::fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
                                            std::span<const std::byte> /*value_null_bitmap*/) {
  const auto row_count = slots.size();
  DebugAssert(value_bytes.size() == row_count * sizeof(RowID), "Row-id tile does not match the slot tile.");

  for (auto row = size_t{0}; row < row_count; ++row) {
    auto& row_id = _row_ids[slots[row]];
    if (row_id.is_null()) {
      std::memcpy(&row_id, value_bytes.data() + row * sizeof(RowID), sizeof(RowID));
    }
  }
}

template <typename ColumnType>
void AnyAccumulatorColumn<ColumnType>::clear() {
  _row_ids.clear();
}

template <typename ColumnType>
void AnyAccumulatorColumn<ColumnType>::combine_from(const AbstractAccumulatorColumn& /*other*/,
                                                    const size_t /*other_first_slot*/,
                                                    std::span<const uint32_t> /*destination_slots*/) {
  Fail("ANY is not eligible for the low-cardinality fast path and must not be combined.");
}

template <typename ColumnType>
void AnyAccumulatorColumn<ColumnType>::finalize_into(const size_t first_slot, const size_t last_slot,
                                                     const size_t output_column_index, OutputColumns& output) const {
  auto& output_column = static_cast<TypedOutputColumn<ColumnType>&>(output.column(output_column_index));
  auto accessor_chunk_id = INVALID_CHUNK_ID;
  auto accessor = std::unique_ptr<AbstractSegmentAccessor<ColumnType>>{};
  for (auto slot = first_slot; slot < last_slot; ++slot) {
    const auto row_id = _row_ids[slot];
    DebugAssert(!row_id.is_null(), "Every dense slot was created by at least one row.");
    if (row_id.chunk_id != accessor_chunk_id) {
      accessor =
          create_segment_accessor<ColumnType>(_input_table->get_chunk(row_id.chunk_id)->get_segment(_source_column));
      accessor_chunk_id = row_id.chunk_id;
    }
    const auto value = accessor->access(row_id.chunk_offset);
    if (value) {
      output_column.append(*value);
    } else {
      output_column.append_null();
    }
  }
}

/**
 * Accumulator for COUNT(DISTINCT): bumps a slot's count only when its DistinctSet reports a first sighting. A group
 * with no non-NULL contributions counts 0, never NULL.
 */
template <typename ColumnType>
class DistinctAccumulatorColumn : public AbstractAccumulatorColumn {
 public:
  void grow_to(size_t slot_count) override;
  void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap) override;
  void clear() override;
  void combine_from(const AbstractAccumulatorColumn& other, size_t other_first_slot,
                    std::span<const uint32_t> destination_slots) override;
  void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                     OutputColumns& output) const override;

 private:
  DistinctSet<ColumnType> _distinct;
  std::vector<int64_t> _counts;  // first sightings per dense slot, as reported by _distinct
};

template <typename ColumnType>
void DistinctAccumulatorColumn<ColumnType>::grow_to(const size_t slot_count) {
  DebugAssert(slot_count >= _counts.size(), "Dense accumulator state only grows.");
  _counts.resize(slot_count, 0);
}

template <typename ColumnType>
void DistinctAccumulatorColumn<ColumnType>::fold(std::span<const uint32_t> slots,
                                                 std::span<const std::byte> value_bytes,
                                                 std::span<const std::byte> value_null_bitmap) {
  const auto row_count = slots.size();
  DebugAssert(value_null_bitmap.empty() || value_null_bitmap.size() * 8 >= row_count,
              "Value-null bitmap does not cover the tile.");
  [[maybe_unused]] constexpr auto VALUE_WIDTH =
      std::is_same_v<ColumnType, pmr_string> ? sizeof(StringValueReference) : sizeof(ColumnType);
  DebugAssert(value_bytes.size() == row_count * VALUE_WIDTH, "Value tile does not match the slot tile.");

  for (auto row = size_t{0}; row < row_count; ++row) {
    if (!value_null_bitmap.empty() && null_bit_set(value_null_bitmap.data(), row)) {
      continue;
    }
    const auto slot = slots[row];
    if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      auto reference = StringValueReference{};
      std::memcpy(&reference, value_bytes.data() + row * sizeof(reference), sizeof(reference));
      const auto value = std::string_view{reinterpret_cast<const char*>(reference.data), reference.length};
      _counts[slot] += _distinct.insert(slot, value) ? 1 : 0;
    } else {
      auto value = ColumnType{};
      std::memcpy(&value, value_bytes.data() + row * sizeof(value), sizeof(value));
      _counts[slot] += _distinct.insert(slot, value) ? 1 : 0;
    }
  }
}

template <typename ColumnType>
void DistinctAccumulatorColumn<ColumnType>::clear() {
  _counts.clear();
  _distinct.clear();
}

template <typename ColumnType>
void DistinctAccumulatorColumn<ColumnType>::combine_from(const AbstractAccumulatorColumn& /*other*/,
                                                         const size_t /*other_first_slot*/,
                                                         std::span<const uint32_t> /*destination_slots*/) {
  Fail("COUNT(DISTINCT) is not eligible for the low-cardinality fast path and must not be combined.");
}

template <typename ColumnType>
void DistinctAccumulatorColumn<ColumnType>::finalize_into(const size_t first_slot, const size_t last_slot,
                                                          const size_t output_column_index,
                                                          OutputColumns& output) const {
  auto& output_column = static_cast<TypedOutputColumn<int64_t>&>(output.column(output_column_index));
  for (auto slot = first_slot; slot < last_slot; ++slot) {
    output_column.append(_counts[slot]);
  }
}

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
   * @throws std::logic_error (via Hyrise Assert/Fail) if an aggregate is unsupported (only
   *   SUM/MIN/MAX/AVG/COUNT/COUNT_DISTINCT/ANY exist) or its (type, function) combination is invalid -- e.g.
   *   SUM(string), whose result type is DataType::Null.
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
    WindowFunction function;    // requested aggregate function (SUM/MIN/MAX/AVG/COUNT/COUNT_DISTINCT/ANY)
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

}  // namespace hyrise
