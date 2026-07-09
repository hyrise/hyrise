#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class Table;
class OutputColumns;

// ============================================================================================================
// Group-by key representation for AggregateDYOD.
//
// A "key" is the group-by column tuple of one row, encoded into a flat, comparable byte buffer. There is deliberately
// NO Key class -- a packed key is just raw bytes living in a scatter store, a merge-map slot, or an output row. All
// behavior lives on a *schema*: a per-query object, built once from the group-by column definitions, that knows how to
// pack a row into key bytes, unpack key bytes back into typed output values, hash a key, and test two keys for
// equality. Everything the hot loops need is a static method on the schema taking raw byte pointers.
//
// Monomorphization. The schema type is selected once per query by resolve_key_schema() (below), which inspects the
// group-by columns and dispatches to one of a bounded set of concrete schema types; the scatter and merge pipelines are
// then instantiated over that one concrete type, so hash/equality compile to fixed, branch-free code. The axes:
//
//   NumericShortKeySchema<Width>   Width in {4,8,12,16} bytes -- numeric-only group-by. hash/equals fixed-size.
//   NumericArbitraryKeySchema      numeric-only group-by wider than 16 bytes; runtime-length hash/equals.
//   MixedKeySchema<LenWidth>       at least one string AND at least one non-string column. LenWidth in {1,2,4,8} is the
//                                  per-string length-prefix field width.
//   StringOnlyKeySchema<LenWidth>  all columns are strings; a MixedKeySchema with a zero-width numeric prefix.
//
// Layout (all four families), contiguous, tightly packed:
//   [ null bitmap | numeric prefix | inline string blob | 8-byte spill pointer ]
// The null bitmap carries one bit per NULLABLE group-by column (present only if any group-by column is nullable) and is
// padded so the fixed part stays a multiple of 4 bytes -- which keeps numeric widths on the {4,8,12,16} buckets and
// leaves no uninitialized interior padding, making whole-buffer equality/hash sound (the pad bytes are zero-filled and
// compared as zero). The string blob and spill pointer are absent for numeric-only schemas.
//
// NULL is carried OUT OF BAND in the null bitmap -- never as an in-band sentinel value. (An in-band "+1 and reserve 0"
// scheme collides at TYPE_MAX in a fixed-width lane, and no byte pattern is safe for a full-range column.) So a NULL
// row's lane/blob content is don't-care; its bit in the null bitmap is what marks it. This unifies NULL handling across
// int, float, and string, and means the value transforms below exist ONLY to make byte-equality match value-equality:
//   * integer lane: bias into unsigned via sign-bit XOR (order-independent, no wider intermediate) -- full range, no
//     reserved value.
//   * float/double lane: canonicalize -0.0 to +0.0 and every NaN to one quiet-NaN pattern, so byte-equality matches
//     numeric equality. No reserved NaN needed for NULL (the bitmap handles it).
//   * string column: length-prefixed (all lengths, then all bytes) canonical form -- NOT delimiter-based, because
//     adversarial embedded-NUL strings make no delimiter safe. No reserved length sentinel for NULL.
// ============================================================================================================

enum class KeyComposition : uint8_t { NumericOnly, Mixed, StringOnly };

// Per-string-column spill storage. String content that does not fit a key's inline blob is copied here and the key
// holds a hash of the content in its inline field plus a pointer into this buffer. Owned privately per partition: one
// instance per (worker, partition) on the scatter side, one per merge partition on the merge side. The merge side
// copies spilled content out of the scatter side on a key's FIRST insertion, so every subsequent deep-compare during
// the fold reads bytes co-located with the cache-resident merge map instead of chasing a pointer into a cold,
// worker-dispersed scatter store.
class StringSpillBuffer : private Noncopyable {
 public:
  // Append length-prefixed content, returning a stable pointer to the stored copy (stable across later appends: the
  // buffer grows by chaining blocks, never relocating live content).
  const std::byte* append(const std::byte* content, size_t length);

  // Drop all content, retaining capacity for reuse across the partitions a worker claims.
  void clear();
};

// ------------------------------------------------------------------------------------------------------------
// Per-column key handlers. One handler per group-by column, resolved once at schema build (concrete subclass per
// DataType / length width) and held by the schema; pack/unpack are called per row. Numeric and string columns are
// separate hierarchies because they occupy different regions and pack differently (a fixed-offset lane vs. a
// length-prefix-plus-content-cursor blob).
// ------------------------------------------------------------------------------------------------------------

// One numeric group-by column's contribution to the key: a fixed-width lane at a byte offset in the numeric prefix.
class AbstractNumericKeyLane {
 public:
  virtual ~AbstractNumericKeyLane() = default;

  // Read the cell and write its transformed bytes into the lane; if the cell is NULL, set this column's bit in
  // null_bitmap instead (lane content is then don't-care).
  virtual void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key,
                    std::byte* null_bitmap) const = 0;

  // Reverse: append this column's value (or a null, per null_bitmap) to its output column.
  virtual void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output,
                      size_t output_column_index, size_t output_row) const = 0;
};

// Concrete numeric lane, one instantiation per group-by column type (int32/int64/float/double). Applies the sign-bit
// XOR bias (integers) or -0.0/NaN canonicalization (floats) described above.
template <typename T>
class NumericKeyLane : public AbstractNumericKeyLane {
 public:
  NumericKeyLane(ColumnID column_id, uint32_t field_offset, uint32_t null_bit_index);

  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key,
            std::byte* null_bitmap) const override;
  void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output, size_t output_column_index,
              size_t output_row) const override;

 private:
  ColumnID _column_id;
  uint32_t _field_offset;
  uint32_t _null_bit_index;  // bit in the key's null bitmap; only consulted if the column is nullable
};

// One string group-by column's contribution: a fixed-offset length prefix plus content written at a running cursor in
// the blob (a column's content offset depends on the actual lengths of the string columns before it), spilling via the
// spill buffer when the blob overflows.
class AbstractStringKeyColumn {
 public:
  virtual ~AbstractStringKeyColumn() = default;

  // Write the length prefix at this column's fixed offset and the bytes at *content_cursor, advancing content_cursor;
  // spill to `spill` when the running blob would overflow. If the cell is NULL, set this column's null bit instead.
  virtual void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key,
                    std::byte*& content_cursor, std::byte* null_bitmap, StringSpillBuffer& spill) const = 0;

  // Reverse: reconstruct this column's string from the length prefix and content (inline or spilled) and append it (or
  // a null) to its output column; advance content_cursor past this column's bytes.
  virtual void unpack(const std::byte* key, const std::byte*& content_cursor, const std::byte* null_bitmap,
                      OutputColumns& output, size_t output_column_index, size_t output_row) const = 0;
};

// Concrete string column, monomorphized over the schema's length-field width.
template <size_t LenWidth>
class StringKeyColumn : public AbstractStringKeyColumn {
 public:
  StringKeyColumn(ColumnID column_id, uint32_t length_field_offset, uint32_t null_bit_index);

  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key, std::byte*& content_cursor,
            std::byte* null_bitmap, StringSpillBuffer& spill) const override;
  void unpack(const std::byte* key, const std::byte*& content_cursor, const std::byte* null_bitmap,
              OutputColumns& output, size_t output_column_index, size_t output_row) const override;

 private:
  ColumnID _column_id;
  uint32_t _length_field_offset;
  uint32_t _null_bit_index;
};

constexpr size_t EXPECTED_GROUP_BY_COLUMNS = 4;
using NumericKeyLanes =
    boost::container::small_vector<std::unique_ptr<AbstractNumericKeyLane>, EXPECTED_GROUP_BY_COLUMNS>;
using StringKeyColumns =
    boost::container::small_vector<std::unique_ptr<AbstractStringKeyColumn>, EXPECTED_GROUP_BY_COLUMNS>;

// ------------------------------------------------------------------------------------------------------------
// Numeric-only, short: total packed width (null bitmap + numeric prefix) fixed at compile time, one instantiation per
// {4,8,12,16}. hash/equals are fixed-size and branch-free; pack/unpack loop the resolved lanes.
// ------------------------------------------------------------------------------------------------------------
template <size_t PackedWidth>
class NumericShortKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr size_t WIDTH = PackedWidth;
  static constexpr bool HAS_STRINGS = false;

  static NumericShortKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;  // == PackedWidth

  // Pack one row's group-by tuple into `key_out` (>= PackedWidth bytes). spill_buffer is unused (no strings) and
  // present only so the call site is uniform across schema variants.
  static void pack(const NumericShortKeySchema& schema, std::span<const AbstractSegment* const> group_by_segments,
                   ChunkOffset chunk_offset, std::byte* key_out, StringSpillBuffer& spill_buffer);
  static void unpack(const NumericShortKeySchema& schema, const std::byte* key, OutputColumns& output,
                     size_t output_row);
  static uint64_t hash(const NumericShortKeySchema& schema, const std::byte* key);          // fixed-size over WIDTH
  static bool equals(const NumericShortKeySchema& schema, const std::byte* a, const std::byte* b);  // memcmp WIDTH

 private:
  NumericKeyLanes _lanes;
};

// ------------------------------------------------------------------------------------------------------------
// Numeric-only, arbitrary width (> 16 bytes): identical interface, fixed width is a runtime value so hash/equals
// operate over packed_width().
// ------------------------------------------------------------------------------------------------------------
class NumericArbitraryKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr bool HAS_STRINGS = false;

  static NumericArbitraryKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;

  static void pack(const NumericArbitraryKeySchema& schema, std::span<const AbstractSegment* const> group_by_segments,
                   ChunkOffset chunk_offset, std::byte* key_out, StringSpillBuffer& spill_buffer);
  static void unpack(const NumericArbitraryKeySchema& schema, const std::byte* key, OutputColumns& output,
                     size_t output_row);
  static uint64_t hash(const NumericArbitraryKeySchema& schema, const std::byte* key);
  static bool equals(const NumericArbitraryKeySchema& schema, const std::byte* a, const std::byte* b);

 private:
  NumericKeyLanes _lanes;
  uint32_t _packed_width{0};
};

// ------------------------------------------------------------------------------------------------------------
// Mixed (>= 1 string and >= 1 non-string column). Fixed part = null bitmap + numeric prefix + inline blob; then the
// 8-byte spill pointer. LenWidth in {1,2,4,8} is the per-string length-field width. The fixed-part width is runtime.
//
// Equality (mode check is load-bearing):
//   1. if the two keys disagree on spill mode (one pointer null, other not) -> NOT equal, never fall through (identical
//      content always makes the identical inline-vs-spill decision, so different modes prove different content; also
//      avoids a deep-compare through a null pointer).
//   2. else memcmp the fixed part; mismatch -> not equal.
//   3. else if inline (pointers null) -> equal.
//   4. else (both spilled, fixed bytes matched only the content-hash) -> deep-compare the spilled bytes.
// hash reuses whatever the inline field holds (raw bytes hashed, or the stored content-hash reused). Equal keys are
// always same-mode, so equal keys always hash equal.
// ------------------------------------------------------------------------------------------------------------
template <size_t LenWidth>
class MixedKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::Mixed;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  static MixedKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;      // fixed part + 8-byte pointer (upper bound of a key's footprint)
  size_t fixed_part_width() const;  // null bitmap + numeric prefix + inline blob, i.e. the memcmp/hash extent

  static void pack(const MixedKeySchema& schema, std::span<const AbstractSegment* const> group_by_segments,
                   ChunkOffset chunk_offset, std::byte* key_out, StringSpillBuffer& spill_buffer);
  static void unpack(const MixedKeySchema& schema, const std::byte* key, OutputColumns& output, size_t output_row);
  static uint64_t hash(const MixedKeySchema& schema, const std::byte* key);
  static bool equals(const MixedKeySchema& schema, const std::byte* a, const std::byte* b);

 private:
  NumericKeyLanes _numeric_lanes;
  StringKeyColumns _string_columns;
  uint32_t _fixed_part_width{0};
};

// ------------------------------------------------------------------------------------------------------------
// String-only: a MixedKeySchema whose numeric prefix has zero width. Same layout (empty prefix), same spill/hash/
// deep-compare equality, same LenWidth axis.
// ------------------------------------------------------------------------------------------------------------
template <size_t LenWidth>
class StringOnlyKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::StringOnly;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  static StringOnlyKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  size_t packed_width() const;
  size_t fixed_part_width() const;

  static void pack(const StringOnlyKeySchema& schema, std::span<const AbstractSegment* const> group_by_segments,
                   ChunkOffset chunk_offset, std::byte* key_out, StringSpillBuffer& spill_buffer);
  static void unpack(const StringOnlyKeySchema& schema, const std::byte* key, OutputColumns& output,
                     size_t output_row);
  static uint64_t hash(const StringOnlyKeySchema& schema, const std::byte* key);
  static bool equals(const StringOnlyKeySchema& schema, const std::byte* a, const std::byte* b);

 private:
  StringKeyColumns _string_columns;
  uint32_t _fixed_part_width{0};
};

// Resolve the concrete key-schema type for a query's group-by columns and invoke `functor` with the fully built schema,
// mirroring resolve_data_type's compile-time dispatch. The operator calls this once per query (only when there is at
// least one group-by column; the zero-group-by case is a separate reduction path) and runs the entire scatter+merge
// pipeline inside the functor, monomorphized over the concrete schema type. DEFINED INLINE in this header (like
// resolve_data_type) -- the functor is a caller-supplied type, so the definition must be visible at the call site.
template <typename Functor>
void resolve_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                        const Functor& functor);

}  // namespace hyrise
