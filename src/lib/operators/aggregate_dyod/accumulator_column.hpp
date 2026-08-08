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

/**
 * One value stream's packing behavior, resolved once per distinct source column at schema build.
 *
 * A value stream reads a source column and serializes it into the per-worker scatter store's value lane during the
 * scatter phase, so the merge phase can later fold the raw bytes without touching the input table again. Numeric
 * streams write the value's native bytes; string streams write a (pointer, length) reference into the per-partition
 * value arena. The concrete subclass is chosen per source column from its data type.
 */
class AbstractValueScatterColumn {
 public:
  virtual ~AbstractValueScatterColumn() = default;

  /**
   * The fixed number of bytes this stream writes to the value lane per row.
   */
  virtual uint32_t element_width() const = 0;
  virtual bool is_nullable() const = 0;

  /**
   * Scatter rows [row_begin, row_end) of one chunk's source column into the worker's store, one typed pass over the
   * window.
   *
   * NULL cells push a zeroed value and set their bit in the row's field of the window's value-null-bitmap scratch,
   * which the caller pushes row-wise after all value streams ran. The window is the one the key pass claimed; the
   * per-row arguments are indexed by window row rather than by chunk offset.
   */
  virtual void scatter(const AbstractSegment& segment, size_t row_begin, size_t row_end,
                       std::span<const PartitionId> row_partitions, size_t stream, ScatterHeads& heads,
                       ScatterStore& store, std::byte* null_bitmap, size_t null_bitmap_width,
                       uint32_t null_bit_index) const = 0;
};

/**
 * Numeric value stream: writes the source value's native bytes with no transform. One instantiation per numeric type.
 *
 * Unlike the key side (where integer lanes are sign-bit-biased and floats canonicalized for correct ordering and
 * equality), values need no ordering, so the bytes are stored verbatim and the accumulator reinterprets them as its
 * native type.
 */
template <typename T>
class NumericValueScatterColumn : public AbstractValueScatterColumn {
 public:
  NumericValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;
  bool is_nullable() const override;
  void scatter(const AbstractSegment& segment, size_t row_begin, size_t row_end,
               std::span<const PartitionId> row_partitions, size_t stream, ScatterHeads& heads, ScatterStore& store,
               std::byte* null_bitmap, size_t null_bitmap_width, uint32_t null_bit_index) const override;

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
void NumericValueScatterColumn<T>::scatter(const AbstractSegment& segment, const size_t row_begin, const size_t row_end,
                                           const std::span<const PartitionId> row_partitions, const size_t stream,
                                           ScatterHeads& heads, ScatterStore& store, std::byte* null_bitmap,
                                           const size_t null_bitmap_width, const uint32_t null_bit_index) const {
  iterate_segment_window<T>(segment, row_begin, row_end, [&](const size_t row, const T* source) {
    auto value = T{};
    if (source) {
      value = *source;
    } else {
      DebugAssert(_nullable, "NULL in a non-nullable value column.");
      set_null_bit(null_bitmap + row * null_bitmap_width, null_bit_index);
    }
    heads.push(store, stream, row_partitions[row], reinterpret_cast<const std::byte*>(&value), sizeof(value));
  });
}

/**
 * String value stream (for MIN/MAX/COUNT on a string column): appends the value's bytes to the per-partition value
 * arena and writes a (pointer, length) reference into the fixed-width stream slot.
 *
 * Unlike string keys, string values are never hashed or compared for equality, so there is no inline-prefix or
 * content-hash optimization here, and the reference holds a stable pointer (StringSpillBuffer never relocates live
 * content) rather than an offset that would need arena-base resolution at fold time.
 */
class StringValueScatterColumn : public AbstractValueScatterColumn {
 public:
  StringValueScatterColumn(ColumnID source_column, bool nullable);

  uint32_t element_width() const override;  // sizeof(pointer, length) reference
  bool is_nullable() const override;
  void scatter(const AbstractSegment& segment, size_t row_begin, size_t row_end,
               std::span<const PartitionId> row_partitions, size_t stream, ScatterHeads& heads, ScatterStore& store,
               std::byte* null_bitmap, size_t null_bitmap_width, uint32_t null_bit_index) const override;

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

inline void StringValueScatterColumn::scatter(const AbstractSegment& segment, const size_t row_begin,
                                              const size_t row_end, const std::span<const PartitionId> row_partitions,
                                              const size_t stream, ScatterHeads& heads, ScatterStore& store,
                                              std::byte* null_bitmap, const size_t null_bitmap_width,
                                              const uint32_t null_bit_index) const {
  iterate_segment_window<pmr_string>(segment, row_begin, row_end, [&](const size_t row, const pmr_string* value) {
    const auto partition = row_partitions[row];
    auto reference = StringValueReference{};
    if (value) {
      reference.data =
          store.value_arena(partition).append(reinterpret_cast<const std::byte*>(value->data()), value->size());
      reference.length = value->size();
    } else {
      DebugAssert(_nullable, "NULL in a non-nullable value column.");
      set_null_bit(null_bitmap + row * null_bitmap_width, null_bit_index);
    }
    heads.push(store, stream, partition, reinterpret_cast<const std::byte*>(&reference), sizeof(reference));
  });
}

/**
 * Merge-side accumulator state for one aggregate: dense and per-slot (SoA), indexed by the merge map's dense slot id.
 *
 * During the merge phase a worker streams every scattered row for a claimed partition through its MergeMap: resolve()
 * maps each key to a dense slot, then fold() accumulates the row's value into that slot. State grows to match the
 * map's slot count and is finalized into the worker's OutputColumns at partition-flush time.
 */
class AbstractAccumulatorColumn {
 public:
  virtual ~AbstractAccumulatorColumn() = default;

  /**
   * Grow dense state to slot_count entries, seeding each new slot with this aggregate's identity element.
   */
  virtual void grow_to(size_t slot_count) = 0;

  /**
   * Fold one tile of rows into their dense slots.
   */
  virtual void fold(std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
                    std::span<const std::byte> value_null_bitmap) = 0;

  /**
   * Drop all dense state while retaining allocated capacity, so the column can be reused for the next partition the
   * worker claims (avoids reallocation churn across partitions).
   */
  virtual void clear() = 0;

  /**
   * Merge another accumulator column's per-slot state into this one, used to combine the per-worker private maps of
   * the low-cardinality path and the sub-maps of a split merge partition. For each row i, the source slot
   * (other_first_slot + i) of `other` is folded into this column's dense slot destination_slots[i].
   */
  virtual void combine_from(const AbstractAccumulatorColumn& other, size_t other_first_slot,
                            std::span<const uint32_t> destination_slots) = 0;

  /**
   * Append the finalized results (per aggregate) for dense slots [first_slot, last_slot) as one contiguous run of
   * output rows.
   */
  virtual void finalize_into(size_t first_slot, size_t last_slot, size_t output_column_index,
                             OutputColumns& output) const = 0;
};

/**
 * Concrete accumulator column, monomorphized over the input column type and the window function.
 */
template <typename ColumnType, WindowFunction Function>
class TypedAccumulatorColumn : public AbstractAccumulatorColumn {
 public:
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

  std::vector<AccumulatorType> _accumulators;
  std::vector<uint32_t> _non_null_counts;
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
 * combine_from() keeps the destination's row id wherever it already has one, since any row of a group is a valid ANY.
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
  std::vector<RowID> _row_ids;
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
void AnyAccumulatorColumn<ColumnType>::combine_from(const AbstractAccumulatorColumn& other_base,
                                                    const size_t other_first_slot,
                                                    const std::span<const uint32_t> destination_slots) {
  const auto& other = static_cast<const AnyAccumulatorColumn&>(other_base);
  const auto row_count = destination_slots.size();
  for (auto row = size_t{0}; row < row_count; ++row) {
    auto& row_id = _row_ids[destination_slots[row]];
    if (row_id.is_null()) {
      row_id = other._row_ids[other_first_slot + row];
    }
  }
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
  std::vector<int64_t> _counts;
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
