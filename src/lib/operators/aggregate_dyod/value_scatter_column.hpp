#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "all_type_variant.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "operators/aggregate_dyod/scatter_store.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/segment_iterate.hpp"
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
      set_null_bit(null_bitmap + (row * null_bitmap_width), null_bit_index);
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
      set_null_bit(null_bitmap + (row * null_bitmap_width), null_bit_index);
    }
    heads.push(store, stream, partition, reinterpret_cast<const std::byte*>(&reference), sizeof(reference));
  });
}

}  // namespace hyrise
