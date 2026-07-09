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

// One output column being built by one worker: a sequence of sealed ValueSegments plus an in-progress buffer.
// append()/append_null() add one value. Sealing is driven externally by OutputColumns (all columns sealed together at
// partition boundaries), NOT auto-triggered per append -- so a column never decides on its own when to seal.
//
// Because each buffer has exactly ONE writer (its owning worker, appending in row order), nulls are written directly
// into the segment's bit-packed representation -- no byte-per-row staging and no atomic masks are needed (both of which
// a shared-output design would have required). Values move into the ValueSegment with no copy at seal.
class AbstractOutputColumn {
 public:
  virtual ~AbstractOutputColumn() = default;

  // Seal the in-progress buffer into a ValueSegment (whatever its current size) and start fresh. Driven by
  // OutputColumns so all of a worker's columns seal at the same row boundary.
  virtual void seal_in_progress() = 0;

  // Rows in the current, not-yet-sealed buffer. Equal across all of a worker's columns at every partition boundary.
  virtual size_t in_progress_row_count() const = 0;

  // Hand over the sealed ValueSegments (in order). Invalidates the builder.
  virtual std::vector<std::shared_ptr<AbstractSegment>> take_segments() = 0;

  // Number of sealed chunks produced so far (equal across all of a worker's columns -- see OutputColumns).
  virtual size_t sealed_chunk_count() const = 0;

 protected:
  AbstractOutputColumn() = default;
};

// Typed output column. append() is called by the typed key-unpack (for group-by columns) and by accumulator finalize
// (for aggregate columns); both know T at their own instantiation and reach this via a downcast from
// AbstractOutputColumn. append() only appends -- it never seals on its own; sealing is decided by OutputColumns between
// partitions.
template <typename T>
class TypedOutputColumn : public AbstractOutputColumn {
 public:
  // `reserve_hint` sizes each fresh in-progress buffer (e.g. the seal threshold); the buffer still grows past it if a
  // single partition overshoots.
  TypedOutputColumn(bool nullable, size_t reserve_hint);

  void append(const T& value);
  void append_null();  // only valid when constructed nullable

  void seal_in_progress() override;
  size_t in_progress_row_count() const override;
  std::vector<std::shared_ptr<AbstractSegment>> take_segments() override;
  size_t sealed_chunk_count() const override;

 private:
  pmr_vector<T> _values;                          // in-progress buffer
  std::optional<pmr_vector<bool>> _null_values;   // written directly; single writer, so no synchronization
  std::vector<std::shared_ptr<AbstractSegment>> _sealed;
  size_t _reserve_hint;
};

// One worker's local output: a typed builder per output column, group-by columns first (in group-by order) then
// aggregate result columns (in aggregate order), matching the operator's output schema.
//
// Invariant: flush_into appends exactly one value to every column per emitted row (nulls included -- a NULL aggregate
// result appends a null, never skips), so at every PARTITION boundary all columns have identical length. Sealing is
// therefore only ever decided between partitions, via maybe_seal(), and seals all columns together -- so their sealed
// segments stay index-aligned (segment k across all columns forms one Chunk) with no per-append bookkeeping. This makes
// the thread-local-then-stitch output correct without any cross-column or cross-worker coordination: the final table is
// just the concatenation of every worker's chunks (at most one partially filled trailing chunk per worker).
class OutputColumns : private Noncopyable {
 public:
  // `seal_threshold` is the in-progress row count past which the next partition boundary triggers a seal; also the
  // per-buffer reserve hint. Chunks come out sized in [seal_threshold, seal_threshold + one partition), so this is a
  // soft target, not an exact chunk size.
  OutputColumns(const TableColumnDefinitions& output_column_definitions, size_t seal_threshold);

  // Access a column builder by output-column index (group-by columns precede aggregate columns).
  AbstractOutputColumn& column(size_t output_column_index);

  // Called by the merge driver after fully flushing one partition: if the current in-progress chunk has grown to or
  // past seal_threshold, seal all columns together and start fresh. A no-op below the threshold.
  void maybe_seal();

  // Seal all columns' in-progress buffers together (end of this worker's work; seals the trailing partial chunk).
  void seal_all();

 private:
  std::vector<std::unique_ptr<AbstractOutputColumn>> _columns;
  size_t _seal_threshold;
};

// Assemble the final output table from every worker's OutputColumns: for each worker, for each sealed chunk index, zip
// the per-column segments at that index into one Chunk and append it. Zero-copy (segments are moved). No compaction of
// partially filled trailing chunks in v1.
std::shared_ptr<Table> build_output_table(const TableColumnDefinitions& output_column_definitions,
                                          std::span<OutputColumns> per_worker_outputs);

}  // namespace hyrise
