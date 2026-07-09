#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"

namespace hyrise {

class OutputColumns;

// Dense Structure-of-Arrays merge accumulator for one partition, ported from the reference PoC's MergeMap and
// generalized from a fixed u128 key to the key schema.
//
//   _table   open-addressing probe index: stores (dense_index + 1), or 0 for empty. Length is a power of two. Probed by
//            the key hash's bits ABOVE the log2(P) partition bits (the low bits are constant within a partition), linear
//            thereafter. Grows independently of the dense storage, so growth re-probes but NEVER moves a dense index --
//            no restart on growth, no compaction on emit.
//   _keys    dense storage of the packed key bytes, `dense_index -> key`, stride = KeySchema::packed_width().
//   _columns one AbstractAccumulatorColumn per aggregate, each dense `dense_index -> accumulator`.
//
// Templated over the concrete key schema so hash/equality inline to fixed, branch-free code; NOT templated over the
// aggregate list -- the accumulator columns are opaque (see accumulator_column.hpp), keeping the aggregate axis out of
// the type. One MergeMap per (worker) instance, reused across the partitions that worker claims via clear().
//
// Cache residency is the point: reserve() sizes the index and dense storage from the per-partition cardinality hint so
// a partition's working set stays cache-resident during the fold.
template <typename KeySchema>
class MergeMap : private Noncopyable {
 public:
  // `shift` is log2(P); `key_schema` drives hashing/equality; the accumulator columns are this map's own mutable state.
  MergeMap(const KeySchema& key_schema, uint32_t shift,
           std::vector<std::unique_ptr<AbstractAccumulatorColumn>> columns);

  // Hint that the map will hold about `distinct_keys` keys: grow the index to keep load under threshold and reserve the
  // dense storage. The index still grows past the hint transparently.
  void reserve(size_t distinct_keys);

  // Resolve each key in `key_tile` (raw bytes, stride KeySchema::packed_width()) to its dense slot, appending the slot
  // to `slots_out`. A previously unseen key is assigned the next dense index, its bytes copied into `_keys`, and every
  // accumulator column grown and identity-seeded. For a spilled string key, the content is interned into `_spill` on
  // this first insertion and the stored key's pointer repointed there -- so later deep-compares read cache-resident
  // bytes rather than chasing into a scatter store (issue: spill handoff / cache locality). This is a batched step: the
  // caller passes one MERGE_TILE_ROWS tile at a time.
  void resolve(std::span<const std::byte> key_tile, std::vector<uint32_t>& slots_out);

  // Fold aggregate `aggregate_index`'s value tile into the slots produced by resolve(). `value_null_bitmap` is the
  // tile's value-null-bitmap (empty when the aggregate's stream is non-nullable). Thin forwarder to the opaque
  // accumulator column, so the per-value typed loop lives in the column and the virtual dispatch is amortized per tile.
  void fold(size_t aggregate_index, std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap);

  // Number of distinct keys (dense slot count).
  size_t size() const;

  // Drop all logical state, retaining capacity, for reuse across the partitions a worker claims. Zeroes the index,
  // clears the dense keys and every accumulator column, and clears the spill buffer.
  void clear();

  // Emit this partition's results as one contiguous run of output rows into `output` (the owning worker's local
  // buffers): unpack each dense key into the group-by output columns and finalize each accumulator column into its
  // aggregate output column. Called once per partition at flush time.
  void flush_into(OutputColumns& output) const;

 private:
  void grow_index();  // double the index and re-probe every dense key; dense indices untouched.

  std::vector<uint32_t> _table;
  size_t _mask{0};
  size_t _max_load{0};
  uint32_t _shift{0};
  std::vector<std::byte> _keys;  // dense, stride KeySchema::packed_width()
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> _columns;
  StringSpillBuffer _spill;  // merge-side interned spill content; unused for numeric schemas
  const KeySchema* _key_schema{nullptr};
};

}  // namespace hyrise
