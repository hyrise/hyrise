#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include "all_type_variant.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "operators/aggregate_dyod/distinct_set.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "operators/aggregate_dyod/value_scatter_column.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

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
template <typename ColumnType, WindowFunction function>
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
  using AccumulatorType = typename WindowFunctionTraits<ColumnType, function>::ReturnType;

  std::vector<AccumulatorType> _accumulators;
  std::vector<uint64_t> _non_null_counts;
};

template <typename ColumnType, WindowFunction function>
TypedAccumulatorColumn<ColumnType, function>::TypedAccumulatorColumn() = default;

template <typename ColumnType, WindowFunction function>
void TypedAccumulatorColumn<ColumnType, function>::grow_to(const size_t slot_count) {
  DebugAssert(slot_count >= _accumulators.size(), "Dense accumulator state only grows.");
  _accumulators.resize(slot_count);
  if constexpr (function != WindowFunction::Count) {
    _non_null_counts.resize(slot_count, 0);
  }
}

template <typename ColumnType, WindowFunction function>
void TypedAccumulatorColumn<ColumnType, function>::fold(std::span<const uint32_t> slots,
                                                        std::span<const std::byte> value_bytes,
                                                        std::span<const std::byte> value_null_bitmap) {
  const auto row_count = slots.size();
  DebugAssert(value_null_bitmap.empty() || value_null_bitmap.size() * 8 >= row_count,
              "Value-null bitmap does not cover the tile.");
  if constexpr (function != WindowFunction::Count) {
    [[maybe_unused]] constexpr auto VALUE_WIDTH =
        std::is_same_v<ColumnType, pmr_string> ? sizeof(StringValueReference) : sizeof(ColumnType);
    DebugAssert(value_bytes.size() == row_count * VALUE_WIDTH, "Value tile does not match the slot tile.");
  }

  if constexpr (function == WindowFunction::Count) {
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
    if constexpr (function == WindowFunction::Count) {
      ++_accumulators[slot];
    } else if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      auto reference = StringValueReference{};
      std::memcpy(&reference, value_bytes.data() + (row * sizeof(reference)), sizeof(reference));
      const auto value = std::string_view{reinterpret_cast<const char*>(reference.data), reference.length};
      auto& count = _non_null_counts[slot];
      auto& current = _accumulators[slot];
      if constexpr (function == WindowFunction::Min) {
        if (count == 0 || value < current) {
          current = pmr_string{value};
        }
      } else if constexpr (function == WindowFunction::Max) {
        if (count == 0 || value > current) {
          current = pmr_string{value};
        }
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    } else {
      auto value = ColumnType{};
      std::memcpy(&value, value_bytes.data() + (row * sizeof(value)), sizeof(value));
      auto& count = _non_null_counts[slot];
      if constexpr (function == WindowFunction::Min) {
        if (count == 0 || value < _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (function == WindowFunction::Max) {
        if (count == 0 || value > _accumulators[slot]) {
          _accumulators[slot] = value;
        }
      } else if constexpr (function == WindowFunction::Sum || function == WindowFunction::Avg) {
        _accumulators[slot] += static_cast<AccumulatorType>(value);
      } else {
        Fail("Unsupported aggregate function.");
      }
      ++count;
    }
  }
}

template <typename ColumnType, WindowFunction function>
void TypedAccumulatorColumn<ColumnType, function>::clear() {
  _accumulators.clear();
  _non_null_counts.clear();
}

template <typename ColumnType, WindowFunction function>
void TypedAccumulatorColumn<ColumnType, function>::combine_from(const AbstractAccumulatorColumn& other_base,
                                                                const size_t other_first_slot,
                                                                const std::span<const uint32_t> destination_slots) {
  const auto& other = static_cast<const TypedAccumulatorColumn&>(other_base);
  const auto row_count = destination_slots.size();
  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto source_slot = other_first_slot + row;
    const auto slot = destination_slots[row];
    if constexpr (function == WindowFunction::Count) {
      _accumulators[slot] += other._accumulators[source_slot];
    } else if constexpr (function == WindowFunction::Sum || function == WindowFunction::Avg) {
      _accumulators[slot] += other._accumulators[source_slot];
      _non_null_counts[slot] += other._non_null_counts[source_slot];
    } else if constexpr (function == WindowFunction::Min || function == WindowFunction::Max) {
      const auto source_count = other._non_null_counts[source_slot];
      if (source_count > 0) {
        if (_non_null_counts[slot] == 0) {
          _accumulators[slot] = other._accumulators[source_slot];
        } else if constexpr (function == WindowFunction::Min) {
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

template <typename ColumnType, WindowFunction function>
void TypedAccumulatorColumn<ColumnType, function>::finalize_into(const size_t first_slot, const size_t last_slot,
                                                                 const size_t output_column_index,
                                                                 OutputColumns& output) const {
  constexpr auto IS_MIN_MAX_OR_SUM =
      function == WindowFunction::Min || function == WindowFunction::Max || function == WindowFunction::Sum;
  auto& output_column = static_cast<TypedOutputColumn<AccumulatorType>&>(output.column(output_column_index));
  for (auto slot = first_slot; slot < last_slot; ++slot) {
    if constexpr (function == WindowFunction::Count) {
      output_column.append(_accumulators[slot]);
    } else if constexpr (function == WindowFunction::Avg) {
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
      std::memcpy(&row_id, value_bytes.data() + (row * sizeof(RowID)), sizeof(RowID));
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
      std::memcpy(&reference, value_bytes.data() + (row * sizeof(reference)), sizeof(reference));
      const auto value = std::string_view{reinterpret_cast<const char*>(reference.data), reference.length};
      _counts[slot] += _distinct.insert(slot, value) ? 1 : 0;
    } else {
      auto value = ColumnType{};
      std::memcpy(&value, value_bytes.data() + (row * sizeof(value)), sizeof(value));
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

}  // namespace hyrise
