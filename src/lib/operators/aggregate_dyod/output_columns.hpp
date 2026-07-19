#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <span>
#include <vector>

#include "storage/table_column_definition.hpp"
#include "types.hpp"

namespace hyrise {

class AbstractSegment;
class Table;

/**
 * One output column built by a single worker during the merge phase: a run of already-sealed ValueSegments followed
 * by one in-progress append buffer.
 *
 * The merge driver finalizes each group into the worker's OutputColumns, which fans the values out to one of these
 * builders per output column. append()/append_null() add a single value; the buffer becomes a ValueSegment only when
 * OutputColumns seals all of a worker's columns together at a partition boundary. A column never decides on its own
 * when to seal.
 *
 * Invariants:
 *   - The in-progress row count is equal across all of a worker's columns at every partition boundary, since
 *     OutputColumns appends exactly one value per column per emitted row.
 *   - Sealed segments are returned in append order and, across a worker's columns, stay index-aligned: segment k of
 *     every column belongs to the same Chunk.
 *
 * Ownership/lifetime/threading: owned by exactly one OutputColumns, itself thread-local to one merge worker. Only that
 * single writer ever touches a builder, and always in row order, so nulls are written straight into the bit-packed
 * ValueSegment representation with no byte-per-row staging and no atomic masks (both of which a shared-output design
 * would require), and values move into the ValueSegment with no copy at seal. Must outlive build_output_table, which
 * drains the sealed segments via take_segments().
 *
 * @see TypedOutputColumn, OutputColumns.
 */
class AbstractOutputColumn {
 public:
  virtual ~AbstractOutputColumn() = default;

  /**
   * Seal the current in-progress buffer into a ValueSegment (at whatever size it has reached) and start a fresh one.
   *
   * @pre Driven by OutputColumns so that all of a worker's columns seal at the same row boundary and their sealed
   *   segments stay index-aligned; runs on the owning merge worker.
   * @post The in-progress row count is 0; the buffered rows are now part of a sealed segment.
   */
  virtual void seal_in_progress() = 0;

  /**
   * The number of rows in the current, not-yet-sealed buffer.
   *
   * @return In-progress row count; equal across all of a worker's columns at every partition boundary. OutputColumns
   *   compares this against its seal threshold.
   */
  virtual size_t in_progress_row_count() const = 0;

  /**
   * Hand the sealed ValueSegments to the caller, in append order, moving them out of the builder.
   *
   * @return The sealed segments in order, empty if none were sealed; ownership transfers to the caller.
   * @pre Any final partial buffer has already been sealed via OutputColumns::seal_all; an unsealed in-progress buffer
   *   is not included.
   * @post Invalidates the builder: afterwards only destruction is valid, and any further append, seal, or read is
   *   undefined.
   * @note Called by build_output_table during final assembly.
   */
  virtual std::vector<std::shared_ptr<AbstractSegment>> take_segments() = 0;

  /**
   * The number of segments sealed so far.
   *
   * @return Sealed-segment count; equal across all of a worker's columns, so it doubles as the worker's sealed-Chunk
   *   count for build_output_table.
   */
  virtual size_t sealed_chunk_count() const = 0;

 protected:
  AbstractOutputColumn() = default;
};

/**
 * Concrete typed output-column builder for element type T.
 *
 * Reached through a downcast from AbstractOutputColumn: the typed key-unpack path calls append() for group-by columns
 * and accumulator finalize calls it for aggregate result columns, each already knowing T at its own instantiation. As
 * in the base contract, append() only appends and never seals on its own; OutputColumns decides sealing between
 * partitions.
 *
 * Ownership/lifetime/threading: single-writer and thread-local to one merge worker (see AbstractOutputColumn). Because
 * only that worker writes, the null buffer needs no synchronization.
 *
 * @see AbstractOutputColumn, OutputColumns.
 */
template <typename T>
class TypedOutputColumn : public AbstractOutputColumn {
 public:
  /**
   * Construct a typed builder.
   *
   * @param nullable Whether this column may hold NULLs. When false, append_null() must never be called and no null
   *   buffer is allocated.
   * @param reserve_hint Rows to reserve for each fresh in-progress buffer, typically the seal threshold. A soft sizing
   *   hint only: the buffer still grows past it if a single partition overshoots.
   */
  TypedOutputColumn(bool nullable, size_t reserve_hint);

  /**
   * Append one non-null value to the in-progress buffer.
   *
   * @param value The finalized value to append; borrowed and copied into the buffer.
   * @post The in-progress row count grows by one; never seals, and the buffer may grow past reserve_hint. If the
   *   column is nullable, the row is also recorded as non-null so the value and null streams stay equal-length.
   */
  void append(const T& value);

  /**
   * Append one NULL to the in-progress buffer, keeping it equal-length with the value stream.
   *
   * @pre The column was constructed nullable; calling this on a non-nullable column is undefined.
   * @post The in-progress row count grows by one.
   * @note Writes directly into the bit-packed null representation, safe without synchronization because the buffer
   *   has a single writer.
   */
  void append_null();

  void seal_in_progress() override;
  size_t in_progress_row_count() const override;
  std::vector<std::shared_ptr<AbstractSegment>> take_segments() override;
  size_t sealed_chunk_count() const override;

 private:
  pmr_vector<T> _values;                         // In-progress append buffer, parallel to _null_values.
  std::optional<pmr_vector<bool>> _null_values;  // Bit-packed null flags (only when nullable); single writer, no sync.
  std::vector<std::shared_ptr<AbstractSegment>> _sealed;  // Sealed segments, in append order.
  size_t _reserve_hint;  // Rows reserved for each fresh in-progress buffer (soft hint).
};

/**
 * One merge worker's thread-local output: one typed builder per output column, ordered group-by columns first (in
 * group-by order) then aggregate result columns (in aggregate order), matching the operator's output schema.
 *
 * During the merge phase the worker finalizes each group and flushes its values here, then calls maybe_seal() between
 * partitions to cut chunks. The thread-local-then-stitch layout is correct without any cross-column or cross-worker
 * coordination: the final table is just the concatenation of every worker's chunks (see build_output_table).
 *
 * Invariants:
 *   - Exactly one value is appended to every column per emitted row (a NULL result appends a null, never skips), so at
 *     every partition boundary all columns have identical length.
 *   - Because of that, sealing is only ever decided between partitions and seals all columns together, keeping their
 *     sealed segments index-aligned: segment k across all columns forms one Chunk, with no per-append bookkeeping.
 *
 * Ownership/lifetime/threading: created and mutated by a single merge worker and not copyable (Noncopyable). Owns its
 * column builders. Must outlive build_output_table, which moves the sealed segments out of it.
 *
 * @see AbstractOutputColumn, TypedOutputColumn, build_output_table.
 */
class OutputColumns : private Noncopyable {
 public:
  /**
   * Construct the per-worker output for a given schema.
   *
   * @param output_column_definitions The operator's output schema (group-by columns first, then aggregate columns),
   *   borrowed for the call; one builder is created per definition with the matching type and nullability.
   * @param seal_threshold In-progress row count at or past which the next partition boundary triggers a seal, and the
   *   per-buffer reserve hint. A soft target: emitted chunks land in [seal_threshold, seal_threshold + one partition).
   */
  OutputColumns(const TableColumnDefinitions& output_column_definitions, size_t seal_threshold);

  /**
   * Access a column builder by output-column index.
   *
   * @param output_column_index Zero-based output-column index; group-by columns precede aggregate columns. Must be in
   *   range for the schema (unchecked).
   * @return Reference to the builder, valid for the lifetime of this OutputColumns.
   */
  AbstractOutputColumn& column(size_t output_column_index);

  /**
   * At a partition boundary, conditionally cut a chunk: if the in-progress row count has reached seal_threshold, seal
   * all columns together and start fresh; otherwise do nothing.
   *
   * @pre Call only after fully flushing a partition, so all columns are equal-length; runs on the owning merge worker.
   * @post Below the threshold nothing changes; at or above it every column has an empty in-progress buffer and one
   *   more sealed segment.
   */
  void maybe_seal();

  /**
   * Seal every column's in-progress buffer together, flushing the trailing partial chunk.
   *
   * @pre All columns are equal-length; called at the end of this worker's work, after the last partition, on the
   *   owning merge worker.
   * @post Every column's in-progress buffer is empty. Call this before build_output_table reads the segments.
   */
  void seal_all();

 private:
  std::vector<std::unique_ptr<AbstractOutputColumn>> _columns;  // One builder per output column, in schema order.
  size_t _seal_threshold;                                       // Soft chunk-size target and per-buffer reserve hint.
};

/**
 * Assemble the final output table from every worker's OutputColumns.
 *
 * For each worker, and for each sealed chunk index, zips that index's per-column segments into one Chunk and appends
 * it to the table. Zero-copy: the segments are moved out of the builders.
 *
 * @param output_column_definitions The operator's output schema; borrowed, used for the table's column layout.
 * @param per_worker_outputs One OutputColumns per merge worker. Their sealed segments are moved out (take_segments),
 *   which invalidates the builders.
 * @pre seal_all() has already run on every element of per_worker_outputs, and all workers share
 *   output_column_definitions.
 * @return The assembled table; up to one partially filled trailing chunk per worker (no compaction in v1).
 */
std::shared_ptr<Table> build_output_table(const TableColumnDefinitions& output_column_definitions,
                                          std::span<OutputColumns> per_worker_outputs);

}  // namespace hyrise
