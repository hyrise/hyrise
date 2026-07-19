#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class AbstractSegment;
class Table;
class OutputColumns;

// ============================================================================================================
// Group-by key representation for AggregateDYOD.
//
// A "key" is the group-by column tuple of one row, encoded into a flat, comparable byte buffer. There is deliberately
// no Key class -- a packed key is just raw bytes living in a scatter store, a merge-map slot, or an output row. All
// behavior lives on a *schema*: a per-query object, built once from the group-by column definitions, that knows how to
// pack a row into key bytes, unpack key bytes back into typed output values, hash a key, and test two keys for
// equality. Everything the hot loops need is a const method on the schema instance taking raw byte pointers.
//
// Monomorphization. The schema type is selected once per query by resolve_key_schema() (below), which inspects the
// group-by columns and dispatches to one of a bounded set of concrete schema types; the scatter and merge pipelines are
// then instantiated over that one concrete type, so hash/equality compile to fixed, branch-free code. The axes:
//
//   NumericShortKeySchema<Width>   Width in {4,8,12,16} bytes -- numeric-only group-by. hash/equals fixed-size.
//   NumericArbitraryKeySchema      numeric-only group-by wider than 16 bytes; runtime-length hash/equals.
//   MixedKeySchema<LenWidth>       at least one string and at least one non-string column. LenWidth in {1,2,4,8} is the
//                                  per-string length-prefix field width.
//   StringOnlyKeySchema<LenWidth>  all columns are strings; a MixedKeySchema with a zero-width numeric prefix.
//
// Layout (all four families), contiguous, tightly packed:
//   [ null bitmap | numeric prefix | inline string blob | 8-byte spill pointer ]
// The null bitmap carries one bit per nullable group-by column (present only if any group-by column is nullable) and is
// padded so the fixed part stays a multiple of 4 bytes -- which keeps numeric widths on the {4,8,12,16} buckets and
// leaves no uninitialized interior padding, making whole-buffer equality/hash sound (the pad bytes are zero-filled and
// compared as zero). The string blob and spill pointer are absent for numeric-only schemas.
//
// NULL is carried out of band in the null bitmap -- never as an in-band sentinel value. (An in-band "+1 and reserve 0"
// scheme collides at TYPE_MAX in a fixed-width lane, and no byte pattern is safe for a full-range column.) A NULL
// cell's lane/blob bytes are deterministically zero (pack() zero-fills the whole key before writing) and are never
// read back as a value; its bit in the null bitmap is what marks it. This unifies NULL handling across
// int, float, and string, and means the value transforms below exist only to make byte-equality match value-equality:
//   * integer lane: bias into unsigned via sign-bit XOR (order-independent, no wider intermediate) -- full range, no
//     reserved value.
//   * float/double lane: canonicalize -0.0 to +0.0 and every NaN to one quiet-NaN pattern, so byte-equality matches
//     numeric equality. No reserved NaN needed for NULL (the bitmap handles it).
//   * string column: length-prefixed (all lengths, then all bytes) canonical form -- not delimiter-based, because
//     adversarial embedded-NUL strings make no delimiter safe. No reserved length sentinel for NULL.
// ============================================================================================================

/**
 * Which column-type families a query's group-by tuple contains. Fixed once per query and used to pick the schema
 * family: NumericOnly -> NumericShort/ArbitraryKeySchema, Mixed -> MixedKeySchema, StringOnly -> StringOnlyKeySchema.
 */
enum class KeyComposition : uint8_t { NumericOnly, Mixed, StringOnly };

/**
 * Overflow storage for string key content that does not fit a key's inline string blob.
 *
 * When a group-by string is too long for the inline blob, its bytes are copied here and the key instead holds a
 * content-hash in its inline field plus an 8-byte pointer into this buffer. This bounds the fixed part of every key
 * while still supporting arbitrarily long strings, and lets equality short-circuit on the hash before a deep compare.
 *
 * Invariants: a pointer returned by append() stays valid for the lifetime of this buffer and is not invalidated by
 * later appends -- the buffer grows by chaining fresh blocks and never relocates live content. clear() invalidates all
 * previously returned pointers.
 *
 * Ownership/lifetime/threading: owned privately per partition and never shared -- one instance per (worker, partition)
 * on the scatter side, one per partition on the merge side; each is touched by a single worker, so no method
 * synchronizes. On a key's first insertion the merge side copies the spilled content out of the scatter-side buffer
 * into its own, so subsequent fold deep-compares read bytes co-located with the cache-resident MergeMap instead of
 * chasing a pointer into a cold, worker-dispersed scatter store. Used in the scatter and merge phases.
 *
 * See StringKeyColumn (writes here on pack) and MixedKeySchema / StringOnlyKeySchema (own the keys that point in).
 */
class StringSpillBuffer : private Noncopyable {
 public:
  /**
   * Copy `length` bytes of string content into the buffer and return a stable pointer to the interned copy.
   *
   * @param content Pointer to the source bytes to copy in; borrowed and read only, not retained (the bytes are copied,
   *   so the source need not outlive the call). Must reference at least `length` readable bytes.
   * @param length Number of bytes to copy.
   * @return Pointer to the interned copy inside this buffer. The pointer stays valid until clear() or destruction and
   *   is never invalidated by later append() calls, because the buffer chains fresh blocks and never relocates live
   *   content.
   * @pre Called by the single owning worker (this buffer is not shared). Runs in the scatter phase (pack spill) and,
   *   on a key's first insertion, on the merge side.
   * Complexity: amortized O(length).
   */
  const std::byte* append(const std::byte* content, size_t length);

  /**
   * Drop all stored content while retaining allocated capacity for reuse.
   *
   * @post All pointers previously returned by append() are invalidated. Allocated blocks are kept so the next batch of
   *   appends reuses them, letting a worker recycle one buffer across the successive partitions it claims.
   * @pre Called by the owning worker only, between uses of the buffer (no live key may still point into it).
   */
  void clear();

 private:
  struct Block {
    std::unique_ptr<std::byte[]> data;
    size_t capacity{0};
    size_t used{0};
  };

  static constexpr size_t MIN_BLOCK_BYTES = 16 * 1024;

  std::vector<Block> _blocks;
  size_t _current_block{0};
};

// ------------------------------------------------------------------------------------------------------------
// Per-column key handlers. Numeric columns get a polymorphic lane (one concrete subclass per DataType), resolved once
// at schema build; string columns are always pmr_string, so they need no type dispatch and are described by a plain
// StringKeyColumn record. The schema drives string packing itself, row-wise, because the spill decision spans all
// string columns of a key (there is a single spill pointer per key; see MixedKeySchema).
// ------------------------------------------------------------------------------------------------------------

// Marks a column that has no bit in the null bitmap because it is not nullable.
constexpr uint32_t NO_NULL_BIT = std::numeric_limits<uint32_t>::max();

/**
 * Polymorphic handler for one numeric group-by column: a fixed-width lane at a byte offset in the numeric prefix.
 *
 * One instance is resolved per numeric group-by column at schema build (concrete subclass NumericKeyLane<T> per
 * DataType) and held by the schema; pack()/unpack() are then called once per row over the resolved lanes.
 *
 * Ownership/lifetime/threading: owned by the schema (via unique_ptr in NumericKeyLanes) and lives for the whole query;
 * stateless and const after construction, so any worker may call pack()/unpack() concurrently on distinct buffers.
 * pack() runs in the scatter phase, unpack() in the merge phase.
 *
 * See NumericKeyLane (the concrete templated lane) and StringKeyColumn (the string-column descriptor).
 */
class AbstractNumericKeyLane {
 public:
  virtual ~AbstractNumericKeyLane() = default;

  /**
   * Read one cell and write its equality-canonical bytes into this lane's fixed field of the key buffer.
   *
   * Integer lanes apply the sign-bit-XOR bias; float/double lanes canonicalize -0.0 and NaN, so whole-buffer
   * byte-equality matches value-equality (see the file banner).
   *
   * @param segment Input segment holding this lane's group-by column; borrowed, read only.
   * @param chunk_offset Row within `segment` to read.
   * @param key Start of the destination key buffer; borrowed, written. Must have room for this lane's field offset
   *   plus its width within the numeric prefix.
   * @param null_bitmap Start of the key's null bitmap region; borrowed, written. If the cell is NULL, this lane sets
   *   its own bit here and leaves the lane bytes untouched -- they stay zero because the schema's pack() zero-fills
   *   the key first, which whole-buffer equality relies on; otherwise the bit is left untouched.
   * @pre Runs in the scatter phase, single-threaded per worker on that worker's own buffer.
   */
  virtual void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key,
                    std::byte* null_bitmap) const = 0;

  /**
   * Reverse of pack(): decode this lane's value and append it (or a NULL) to its output column.
   *
   * @param key Start of the packed key to read; borrowed, read only.
   * @param null_bitmap Start of the key's null bitmap; borrowed, read only. If this lane's bit is set, a NULL is
   *   appended and the lane bytes are ignored; otherwise the bias/canonicalization of pack() is inverted.
   * @param output Destination columns for the emitting worker; borrowed, written. Thread-local with a single writer.
   * @param output_column_index Index of the output column this lane feeds.
   * @param output_row 0-based index of the output row being emitted.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own OutputColumns.
   * @post Exactly one value (a real value or a NULL) is appended, keeping all output columns equal-length.
   */
  virtual void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output,
                      size_t output_column_index, size_t output_row) const = 0;
};

/**
 * Concrete numeric lane, monomorphized over the column's C++ type T (int32_t/int64_t/float/double).
 *
 * Implements AbstractNumericKeyLane: pack() applies the sign-bit-XOR bias for integers or the -0.0/NaN
 * canonicalization for floats, and unpack() inverts it. See the file banner for why these transforms exist.
 *
 * Ownership/lifetime/threading: as AbstractNumericKeyLane -- owned by the schema, const after construction, safely
 * shared across workers.
 */
template <typename T>
class NumericKeyLane : public AbstractNumericKeyLane {
 public:
  /**
   * @param column_id ColumnID of the source group-by column in the input table.
   * @param field_offset Byte offset of this lane's field within the key buffer (within the numeric prefix).
   * @param null_bit_index Bit index of this column within the key's null bitmap; NO_NULL_BIT if the column is not
   *   nullable.
   */
  NumericKeyLane(ColumnID column_id, uint32_t field_offset, uint32_t null_bit_index);

  void pack(const AbstractSegment& segment, ChunkOffset chunk_offset, std::byte* key,
            std::byte* null_bitmap) const override;
  void unpack(const std::byte* key, const std::byte* null_bitmap, OutputColumns& output, size_t output_column_index,
              size_t output_row) const override;

 private:
  ColumnID _column_id;       // ColumnID of the source group-by column in the input table.
  uint32_t _field_offset;    // Byte offset of this lane's field within the key buffer (numeric prefix).
  uint32_t _null_bit_index;  // Bit in the key's null bitmap; NO_NULL_BIT if the column is not nullable.
};

/**
 * Descriptor for one string group-by column: where its length-prefix field lives and which null bit it owns.
 *
 * Unlike the numeric lanes there is no polymorphic handler: string cells are always pmr_string, so there is no type
 * axis to dispatch on. The schema drives string packing/unpacking itself, row-wise, because the inline-vs-spill
 * decision spans all string columns of a key (a key carries a single spill pointer; see MixedKeySchema).
 */
struct StringKeyColumn {
  ColumnID column_id;            // ColumnID of the source group-by column in the input table.
  uint32_t tuple_index;          // Position of this column within the group-by tuple / output row.
  uint32_t length_field_offset;  // Byte offset of this column's length-prefix field within the key buffer.
  uint32_t null_bit_index;       // Bit in the key's null bitmap; NO_NULL_BIT if the column is not nullable.
};

// Inline capacity for the per-schema lane/column vectors: most group-bys have at most this many columns, so the lanes
// stay inline (no heap allocation) in the common case.
constexpr size_t EXPECTED_GROUP_BY_COLUMNS = 4;
using NumericKeyLanes =
    boost::container::small_vector<std::unique_ptr<AbstractNumericKeyLane>, EXPECTED_GROUP_BY_COLUMNS>;
using StringKeyColumns = boost::container::small_vector<StringKeyColumn, EXPECTED_GROUP_BY_COLUMNS>;
using KeyTupleIndices = boost::container::small_vector<uint32_t, EXPECTED_GROUP_BY_COLUMNS>;

// ------------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------------
/**
 * Numeric-only group-by key schema whose total packed width is the compile-time constant PackedWidth.
 *
 * Selected by resolve_key_schema when every group-by column is numeric and the packed width (null bitmap + numeric
 * prefix) is one of {4,8,12,16} bytes; one template instantiation per bucket. Because the width is known at compile
 * time, hash() and equals() are fixed-size and branch-free, while pack()/unpack() loop the resolved lanes.
 *
 * Invariants: packed_width() == PackedWidth for every key; keys carry no string blob or spill pointer.
 *
 * Ownership/lifetime/threading: one immutable instance per query, built by build() and shared read-only by all
 * workers across the scatter (pack) and merge (unpack/hash/equals) phases.
 *
 * See NumericArbitraryKeySchema (same interface for widths > 16 bytes) and the file banner for the byte layout.
 */
template <size_t PackedWidth>
class NumericShortKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr size_t WIDTH = PackedWidth;
  static constexpr bool HAS_STRINGS = false;

  /**
   * Build the schema for a query's group-by columns: resolve one NumericKeyLane per column and lay out the fields.
   *
   * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only.
   * @param input_table Table providing the columns' data types; borrowed, read only.
   * @return A fully built schema whose packed_width() equals PackedWidth.
   * @pre Every listed column is numeric and the resolved packed width equals PackedWidth (resolve_key_schema
   *   guarantees this before instantiating the template).
   */
  static NumericShortKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  /** @return The packed key width in bytes; always the compile-time constant PackedWidth. */
  size_t packed_width() const;
  /** @return The number of group-by columns this schema packs, which is also the group-by output column count. */
  size_t column_count() const;

  /**
   * Pack one input row's group-by tuple into a key buffer.
   *
   * @param group_by_segments Segments for the group-by columns of the current chunk, in schema order; borrowed, read
   *   only.
   * @param chunk_offset Row within those segments to pack.
   * @param key_out Destination key buffer; borrowed, written. Must be at least PackedWidth bytes.
   * @param spill_buffer Unused for numeric-only keys (there are no strings); present only so the call site is uniform
   *   across schema variants.
   * @pre Runs in the scatter phase, single-threaded per worker on that worker's own store.
   * @post Every one of the packed_width() destination bytes is initialized; bytes not covered by a value (NULL lanes,
   *   padding) are zero, which whole-buffer equality and hashing rely on.
   */
  void pack(std::span<const AbstractSegment* const> group_by_segments, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  /**
   * Unpack a packed key back into typed output values, one appended cell per group-by column.
   *
   * @param key Packed key to decode; borrowed, read only. Must be at least PackedWidth bytes.
   * @param output Destination columns for the emitting worker; borrowed, written. Thread-local, single writer.
   * @param output_row 0-based index of the output row being emitted.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own OutputColumns.
   */
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  /**
   * Hash a packed key over its full fixed width.
   *
   * @param key Packed key to hash; borrowed, read only. Must be at least PackedWidth bytes.
   * @return 64-bit hash of the PackedWidth key bytes. Equal keys always hash equal (whole-buffer byte-equality).
   * Complexity: O(PackedWidth), fixed-size and branch-free.
   */
  uint64_t hash(const std::byte* key) const;
  /**
   * Test two packed keys for equality by comparing their full fixed width.
   *
   * @param a First packed key; borrowed, read only. Must be at least PackedWidth bytes.
   * @param b Second packed key; borrowed, read only. Must be at least PackedWidth bytes.
   * @return true iff the PackedWidth bytes of `a` and `b` are identical (a plain memcmp; NULLs and the lane value
   *   transforms already make byte-equality match value-equality).
   * Complexity: O(PackedWidth), fixed-size.
   */
  bool equals(const std::byte* a, const std::byte* b) const;

 private:
  NumericKeyLanes _lanes;  // One lane per numeric group-by column, in schema order.
};

// ------------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------------
/**
 * Numeric-only group-by key schema for widths greater than 16 bytes, where the width is a runtime value.
 *
 * Same interface and semantics as NumericShortKeySchema, but because the packed width is not a compile-time constant,
 * hash() and equals() operate over packed_width() rather than a fixed WIDTH. Selected by resolve_key_schema when all
 * group-by columns are numeric but the packed width exceeds the largest NumericShortKeySchema bucket.
 *
 * Invariants: keys carry no string blob or spill pointer; every key is exactly packed_width() bytes.
 *
 * Ownership/lifetime/threading: one immutable instance per query, shared read-only by all workers across the scatter
 * and merge phases.
 */
class NumericArbitraryKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::NumericOnly;
  static constexpr bool HAS_STRINGS = false;

  /**
   * Build the schema for a query's group-by columns: resolve one NumericKeyLane per column and record the width.
   *
   * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only.
   * @param input_table Table providing the columns' data types; borrowed, read only.
   * @return A fully built schema whose packed_width() is the resolved runtime width.
   * @pre Every listed column is numeric and the resolved packed width exceeds 16 bytes.
   */
  static NumericArbitraryKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  /** @return The packed key width in bytes, fixed for this query but known only at runtime. */
  size_t packed_width() const;
  /** @return The number of group-by columns this schema packs, which is also the group-by output column count. */
  size_t column_count() const;

  /**
   * Pack one input row's group-by tuple into a key buffer.
   *
   * @param group_by_segments Segments for the group-by columns of the current chunk, in schema order; borrowed, read
   *   only.
   * @param chunk_offset Row within those segments to pack.
   * @param key_out Destination key buffer; borrowed, written. Must be at least packed_width() bytes.
   * @param spill_buffer Unused for numeric-only keys; present only for a uniform call site across schema variants.
   * @pre Runs in the scatter phase, single-threaded per worker on that worker's own store.
   * @post Every one of the packed_width() destination bytes is initialized; bytes not covered by a value (NULL lanes,
   *   padding) are zero, which whole-buffer equality and hashing rely on.
   */
  void pack(std::span<const AbstractSegment* const> group_by_segments, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  /**
   * Unpack a packed key back into typed output values, one appended cell per group-by column.
   *
   * @param key Packed key to decode; borrowed, read only. Must be at least packed_width() bytes.
   * @param output Destination columns for the emitting worker; borrowed, written. Thread-local, single writer.
   * @param output_row 0-based index of the output row being emitted.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own OutputColumns.
   */
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  /**
   * Hash a packed key over its full runtime width.
   *
   * @param key Packed key to hash; borrowed, read only. Must be at least packed_width() bytes.
   * @return 64-bit hash of the packed_width() key bytes. Equal keys always hash equal.
   * Complexity: O(packed_width()).
   */
  uint64_t hash(const std::byte* key) const;
  /**
   * Test two packed keys for equality by comparing their full runtime width.
   *
   * @param a First packed key; borrowed, read only. Must be at least packed_width() bytes.
   * @param b Second packed key; borrowed, read only. Must be at least packed_width() bytes.
   * @return true iff the packed_width() bytes of `a` and `b` are identical (a plain memcmp).
   * Complexity: O(packed_width()).
   */
  bool equals(const std::byte* a, const std::byte* b) const;

 private:
  NumericKeyLanes _lanes;     // One lane per numeric group-by column, in schema order.
  uint32_t _packed_width{0};  // Fixed packed width in bytes for this query, computed at build().
};

// ------------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------------
/**
 * Group-by key schema for a mix of at least one string and at least one non-string column.
 *
 * Layout: a runtime-width fixed part (null bitmap + numeric prefix + inline string blob) followed by an 8-byte spill
 * pointer. LenWidth in {1,2,4,8} is the per-string length-prefix field width; the fixed-part width is a runtime value.
 *
 * Equality (the spill-mode check is load-bearing):
 *   1. If the two keys disagree on spill mode (one pointer null, the other not) they are not equal and never fall
 *      through: identical content always makes the identical inline-vs-spill decision, so different modes prove
 *      different content, and this also avoids a deep compare through a null pointer.
 *   2. Otherwise memcmp the fixed part; a mismatch means not equal.
 *   3. Otherwise, if inline (both pointers null), they are equal.
 *   4. Otherwise (both spilled, and the fixed bytes matched only on the content-hash) deep-compare the spilled bytes.
 * hash() reuses whatever the inline field holds (raw bytes hashed, or the stored content-hash reused). Equal keys are
 * always same-mode, so equal keys always hash equal.
 *
 * Invariants: every key ends with the 8-byte spill pointer; the fixed part is fixed_part_width() bytes and is the sole
 * memcmp/hash extent.
 *
 * Ownership/lifetime/threading: one immutable instance per query, shared read-only by all workers; spilled content it
 * points into lives in per-partition StringSpillBuffers. Used in the scatter (pack) and merge (unpack/hash/equals)
 * phases.
 *
 * Only LenWidth 4 is explicitly instantiated for now (see resolve_key_schema); the merge side re-interns spilled
 * content on a key's first insertion via reintern_spill() (see MergeMap).
 *
 * See StringOnlyKeySchema (this schema with a zero-width numeric prefix) and StringSpillBuffer.
 */
template <size_t LenWidth>
class MixedKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::Mixed;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  /**
   * Build the schema: resolve numeric lanes and string columns, lay out the fixed part, and record its width.
   *
   * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only.
   * @param input_table Table providing the columns' data types; borrowed, read only.
   * @return A fully built schema with at least one string and at least one numeric column.
   * @pre The columns include at least one string and at least one non-string column (resolve_key_schema guarantees
   *   this before instantiating the template).
   */
  static MixedKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  /** @return Upper bound of a key's footprint in bytes: fixed_part_width() plus the 8-byte spill pointer. */
  size_t packed_width() const;
  /**
   * @return Width in bytes of the fixed part (null bitmap + numeric prefix + inline string blob), which is exactly the
   *   extent that hash() and equals() memcmp.
   */
  size_t fixed_part_width() const;
  /** @return The number of group-by columns this schema packs, which is also the group-by output column count. */
  size_t column_count() const;

  /**
   * Pack one input row's group-by tuple into a key buffer, spilling overlong strings as needed.
   *
   * @param group_by_segments Segments for the group-by columns of the current chunk, in schema order; borrowed, read
   *   only.
   * @param chunk_offset Row within those segments to pack.
   * @param key_out Destination key buffer; borrowed, written. Must be at least packed_width() bytes.
   * @param spill_buffer Per-partition overflow buffer for string content that does not fit the inline blob; borrowed,
   *   may be appended to. Must be the buffer owned by the calling worker/partition.
   * @pre Runs in the scatter phase, single-threaded per worker on that worker's own store and spill buffer.
   * @post Every one of the packed_width() destination bytes is initialized; bytes not covered by a value (NULL lanes,
   *   padding, the blob tail, an unused spill pointer) are zero, which whole-buffer equality and hashing rely on.
   */
  void pack(std::span<const AbstractSegment* const> group_by_segments, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  /**
   * Unpack a packed key back into typed output values, one appended cell per group-by column.
   *
   * @param key Packed key to decode; borrowed, read only. Any spilled string content it points to must still be live.
   * @param output Destination columns for the emitting worker; borrowed, written. Thread-local, single writer.
   * @param output_row 0-based index of the output row being emitted.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own OutputColumns.
   */
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  /**
   * Hash a packed key over its fixed part.
   *
   * @param key Packed key to hash; borrowed, read only.
   * @return 64-bit hash over the fixed part, which covers the inline string bytes or, for a spilled key, the stored
   *   content-hash of the spilled content. Equal keys always hash equal because equal keys are always same-mode.
   * Complexity: O(fixed_part_width()).
   */
  uint64_t hash(const std::byte* key) const;
  /**
   * Test two packed keys for equality using the four-step spill-mode-aware protocol documented on the class.
   *
   * @param a First packed key; borrowed, read only. Spilled content it points to must be live.
   * @param b Second packed key; borrowed, read only. Spilled content it points to must be live.
   * @return true iff the keys are equal: same spill mode, matching fixed part, and (when spilled) matching deep bytes.
   * Complexity: O(fixed_part_width()) inline, plus O(string length) for the deep compare when both keys are spilled.
   */
  bool equals(const std::byte* a, const std::byte* b) const;
  /**
   * Re-intern a spilled key's string content into `spill_buffer` and repoint the key's spill pointer there.
   *
   * Called by the merge side on a key's first insertion, so later deep compares read bytes co-located with the
   * cache-resident merge map instead of chasing a pointer into a cold scatter-side buffer. A no-op for inline keys.
   *
   * @param key Packed key to re-intern; borrowed, its spill pointer is rewritten. The spilled content it points to
   *   must still be live.
   * @param spill_buffer The merge side's own buffer; borrowed, appended to.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own map and buffer.
   */
  void reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const;

 private:
  NumericKeyLanes _numeric_lanes;          // One lane per non-string group-by column, in schema order.
  KeyTupleIndices _numeric_tuple_indices;  // Each lane's position within the group-by tuple / output row.
  StringKeyColumns _string_columns;        // One descriptor per string group-by column, in schema order.
  uint32_t _blob_offset{0};                // Byte offset of the inline string blob within the key buffer.
  uint32_t _fixed_part_width{0};           // Memcmp/hash extent in bytes, computed at build().
};

// ------------------------------------------------------------------------------------------------------------
// ------------------------------------------------------------------------------------------------------------
/**
 * Group-by key schema for a tuple of only string columns: a MixedKeySchema whose numeric prefix has zero width.
 *
 * Same byte layout (with an empty numeric prefix), same spill/hash/deep-compare equality, and same LenWidth axis as
 * MixedKeySchema; see that class for the equality protocol and hash rationale. Selected by resolve_key_schema when
 * every group-by column is a string.
 *
 * Invariants and ownership/lifetime/threading match MixedKeySchema (one immutable per-query instance, shared read-only
 * across the scatter and merge phases; spilled content lives in per-partition StringSpillBuffers). As with
 * MixedKeySchema, only LenWidth 4 is explicitly instantiated for now.
 */
template <size_t LenWidth>
class StringOnlyKeySchema {
 public:
  static constexpr KeyComposition COMPOSITION = KeyComposition::StringOnly;
  static constexpr bool HAS_STRINGS = true;
  static constexpr size_t LENGTH_FIELD_WIDTH = LenWidth;

  /**
   * Build the schema: resolve one string column per group-by column and lay out the fixed part.
   *
   * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only.
   * @param input_table Table providing the columns' data types; borrowed, read only.
   * @return A fully built schema in which every group-by column is a string.
   * @pre Every listed column is a string (resolve_key_schema guarantees this before instantiating the template).
   */
  static StringOnlyKeySchema build(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

  /** @return Upper bound of a key's footprint in bytes: fixed_part_width() plus the 8-byte spill pointer. */
  size_t packed_width() const;
  /** @return Width in bytes of the fixed part (null bitmap + inline string blob), the memcmp/hash extent. */
  size_t fixed_part_width() const;
  /** @return The number of group-by columns this schema packs, which is also the group-by output column count. */
  size_t column_count() const;

  /**
   * Pack one input row's group-by tuple into a key buffer, spilling overlong strings as needed.
   *
   * @param group_by_segments Segments for the group-by columns of the current chunk, in schema order; borrowed, read
   *   only.
   * @param chunk_offset Row within those segments to pack.
   * @param key_out Destination key buffer; borrowed, written. Must be at least packed_width() bytes.
   * @param spill_buffer Per-partition overflow buffer for string content that does not fit the inline blob; borrowed,
   *   may be appended to. Must be the buffer owned by the calling worker/partition.
   * @pre Runs in the scatter phase, single-threaded per worker on that worker's own store and spill buffer.
   * @post Every one of the packed_width() destination bytes is initialized; bytes not covered by a value (NULL lanes,
   *   padding, the blob tail, an unused spill pointer) are zero, which whole-buffer equality and hashing rely on.
   */
  void pack(std::span<const AbstractSegment* const> group_by_segments, ChunkOffset chunk_offset, std::byte* key_out,
            StringSpillBuffer& spill_buffer) const;
  /**
   * Unpack a packed key back into typed output values, one appended string per group-by column.
   *
   * @param key Packed key to decode; borrowed, read only. Any spilled string content it points to must still be live.
   * @param output Destination columns for the emitting worker; borrowed, written. Thread-local, single writer.
   * @param output_row 0-based index of the output row being emitted.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own OutputColumns.
   */
  void unpack(const std::byte* key, OutputColumns& output, size_t output_row) const;
  /**
   * Hash a packed key over its fixed part.
   *
   * @param key Packed key to hash; borrowed, read only.
   * @return 64-bit hash over the fixed part, which covers the inline string bytes or, for a spilled key, the stored
   *   content-hash of the spilled content. Equal keys always hash equal.
   * Complexity: O(fixed_part_width()).
   */
  uint64_t hash(const std::byte* key) const;
  /**
   * Test two packed keys for equality using the same spill-mode-aware protocol as MixedKeySchema.
   *
   * @param a First packed key; borrowed, read only. Spilled content it points to must be live.
   * @param b Second packed key; borrowed, read only. Spilled content it points to must be live.
   * @return true iff the keys are equal: same spill mode, matching fixed part, and (when spilled) matching deep bytes.
   * Complexity: O(fixed_part_width()) inline, plus O(string length) for the deep compare when both keys are spilled.
   */
  bool equals(const std::byte* a, const std::byte* b) const;
  /**
   * Re-intern a spilled key's string content into `spill_buffer` and repoint the key's spill pointer there; see
   * MixedKeySchema::reintern_spill. A no-op for inline keys.
   *
   * @param key Packed key to re-intern; borrowed, its spill pointer is rewritten. The spilled content it points to
   *   must still be live.
   * @param spill_buffer The merge side's own buffer; borrowed, appended to.
   * @pre Runs in the merge phase, single-threaded per worker on that worker's own map and buffer.
   */
  void reintern_spill(std::byte* key, StringSpillBuffer& spill_buffer) const;

 private:
  StringKeyColumns _string_columns;  // One descriptor per string group-by column, in schema order.
  uint32_t _blob_offset{0};          // Byte offset of the inline string blob within the key buffer.
  uint32_t _fixed_part_width{0};     // Memcmp/hash extent in bytes, computed at build().
};

/**
 * The schema family and short-width bucket resolve_key_schema dispatches on, computed by choose_key_schema.
 */
struct KeySchemaChoice {
  KeyComposition composition{KeyComposition::NumericOnly};
  size_t short_packed_width{0};  // One of {4,8,12,16} for NumericShortKeySchema; 0 when the width exceeds the buckets.
};

/**
 * Inspect the group-by columns and compute which schema family (and, for numeric-only tuples, which short-width
 * bucket) fits them; the type dispatch itself happens in resolve_key_schema.
 *
 * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only.
 * @param input_table Table providing the columns' data types; borrowed, read only.
 * @return The composition and, for numeric-only tuples of at most 16 bytes, the packed-width bucket.
 */
KeySchemaChoice choose_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table);

/**
 * Resolve the concrete key-schema type for a query's group-by columns and invoke `functor` with the built schema.
 *
 * Inspects the group-by columns, selects one of NumericShortKeySchema / NumericArbitraryKeySchema / MixedKeySchema /
 * StringOnlyKeySchema, builds it, and calls functor with that concrete instance, mirroring resolve_data_type's
 * compile-time dispatch. The entire scatter+merge pipeline runs inside the functor, monomorphized over the concrete
 * schema type so pack/unpack/hash/equals compile to fixed, branch-free code.
 *
 * @tparam Functor Caller-supplied callable; invoked as functor(schema) with the concrete schema type deduced.
 * @param group_by_column_ids ColumnIDs of the group-by columns, in output order; borrowed, read only. Must be
 *   non-empty -- the zero-group-by case is a separate reduction path and must not reach here.
 * @param input_table Table providing the columns' data types; borrowed, read only.
 * @param functor The callable to run once with the built schema; borrowed.
 * @pre There is at least one group-by column.
 * @note Defined inline in this header (like resolve_data_type) because the functor is a caller-supplied type whose
 *   instantiation must be visible at the call site.
 */
template <typename Functor>
void resolve_key_schema(const std::vector<ColumnID>& group_by_column_ids, const Table& input_table,
                        const Functor& functor) {
  Assert(!group_by_column_ids.empty(), "resolve_key_schema requires at least one group-by column.");
  // For now, string length fields are always 4 bytes wide.
  const auto choice = choose_key_schema(group_by_column_ids, input_table);
  switch (choice.composition) {
    case KeyComposition::NumericOnly:
      switch (choice.short_packed_width) {
        case 4:
          functor(NumericShortKeySchema<4>::build(group_by_column_ids, input_table));
          return;
        case 8:
          functor(NumericShortKeySchema<8>::build(group_by_column_ids, input_table));
          return;
        case 12:
          functor(NumericShortKeySchema<12>::build(group_by_column_ids, input_table));
          return;
        case 16:
          functor(NumericShortKeySchema<16>::build(group_by_column_ids, input_table));
          return;
        default:
          functor(NumericArbitraryKeySchema::build(group_by_column_ids, input_table));
          return;
      }
    case KeyComposition::Mixed:
      functor(MixedKeySchema<4>::build(group_by_column_ids, input_table));
      return;
    case KeyComposition::StringOnly:
      functor(StringOnlyKeySchema<4>::build(group_by_column_ids, input_table));
      return;
  }
  Fail("Invalid KeyComposition.");
}

}  // namespace hyrise
