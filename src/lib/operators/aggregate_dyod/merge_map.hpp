#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "operators/aggregate_dyod/accumulator_column.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * A dense, structure-of-arrays hash accumulator that merges one radix partition's scattered rows into grouped
 * aggregates. Ported from the reference PoC's MergeMap and generalized from a fixed u128 key to an arbitrary KeySchema.
 *
 * This is the core of the merge phase. A merge worker streams every scatter-store row belonging to one partition
 * through this map in merge_tile_rows() tiles: resolve() maps each packed key to a dense slot, fold() folds the row's
 * values into that slot's accumulators, and flush_into() finalizes the groups into the worker's thread-local
 * OutputColumns. The dense slot id is the shared index across three parallel arrays, so grouping happens once (in
 * resolve) and every aggregate folds against the same slots.
 *
 * Internal representation (dense slot id `d` addresses all three):
 *   _table   open-addressing probe index: stores (d + 1), or 0 for empty. Length is a power of two. Probed by the key
 *            hash's bits above the log2(P) partition bits (the low bits are constant within a partition), linear
 *            thereafter. Grows independently of the dense storage, so growth re-probes but never moves a dense index --
 *            no restart on growth, no compaction on emit.
 *   _keys    dense storage of the packed key bytes, `d -> key`, stride = KeySchema::packed_width().
 *   _columns one AbstractAccumulatorColumn per aggregate (accumulator_column.hpp), each dense `d -> accumulator`.
 *
 * Templated over the concrete key schema so hash/equality inline to fixed, branch-free code; not templated over the
 * aggregate list -- the accumulator columns are opaque, keeping the aggregate axis out of the type.
 *
 * Cache residency is the point: reserve() sizes the index and dense storage from the per-partition cardinality hint
 * (keys_budget(), aggregate_dyod_config.hpp) so a partition's working set stays cache-resident during the fold.
 *
 * Invariants:
 *   * _table holds dense-slot-plus-one biased values; 0 means empty, so a stored slot id is always the entry minus 1.
 *   * _table length is a power of two and _mask == length - 1; _keys and every _columns entry stay equal in length to
 *     size() (one dense slot per distinct key).
 *   * A dense index, once assigned to a key, never moves until clear() (grow_index() re-probes but does not relocate).
 *   * After resolve() a slot exists for every returned id, so a subsequent fold() into those slots never grows storage.
 *
 * Ownership/lifetime/threading:
 *   One MergeMap per merge worker, exclusively owned and single-threaded -- no method is safe to call concurrently on
 *   the same instance. Reused across the partitions that worker claims via clear() (retains capacity). Owns its
 *   accumulator columns (moved in at construction) and its merge-side _spill buffer; borrows the KeySchema (a const
 *   pointer that must outlive the map). Lives and is touched only in the merge phase, after the scatter barrier has
 *   made every worker's ScatterStore (scatter_store.hpp) rows visible. Related: KeySchema (key_schema.hpp),
 *   AbstractAccumulatorColumn (accumulator_column.hpp), OutputColumns (output_columns.hpp).
 */
template <typename KeySchema>
class MergeMap : private Noncopyable {
 public:
  /**
   * Construct an empty merge map for one partition.
   *
   * @param key_schema  Borrowed schema driving key hashing, equality, packing, and unpacking; must outlive this map.
   * @param shift       log2(P), the partition-count exponent. The probe index consults the key hash's bits above these
   *   low `shift` bits, since the low bits are constant within a single partition.
   * @param columns     One accumulator column per aggregate, in aggregate order; consumed (moved in) and thereafter
   *   owned as this map's mutable per-slot state.
   * @post The map is empty (size() == 0) with no reserved capacity; call reserve() before folding to size storage.
   */
  MergeMap(const KeySchema& key_schema, uint32_t shift,
           std::vector<std::unique_ptr<AbstractAccumulatorColumn>> columns);

  /**
   * Size the probe index and dense storage for an expected `distinct_keys` keys, keeping the fold cache-resident.
   *
   * @param distinct_keys  Expected number of distinct keys in this partition (the per-partition cardinality hint, from
   *   the estimate phase and keys_budget()). A hint only: the index still grows past it transparently via grow_index().
   * @pre Call before resolve() for a partition, typically once after clear() when reusing the map.
   * @post The index is grown so its load stays under threshold at `distinct_keys`, and dense storage is reserved.
   */
  void reserve(size_t distinct_keys);

  /**
   * Resolve a tile of packed keys to their dense slots, creating slots for keys not yet seen in this fill.
   *
   * Each key is hashed and probed. A previously unseen key is assigned the next dense index, its bytes copied into the
   * dense key storage, and every accumulator column grown and identity-seeded for the new slot. For a spilled string
   * key, the content is interned into this map's merge-side spill buffer on that first insertion and the stored key's
   * spill pointer repointed there, so later deep-compares read cache-resident bytes rather than chasing a pointer into
   * a cold, worker-dispersed scatter store.
   *
   * @param key_tile   Raw packed key bytes for the tile, stride KeySchema::packed_width(); length must be a whole
   *   multiple of the stride and hold at most merge_tile_rows() rows.
   * @param slots_out  Receives one dense slot id per key, appended in row order; resolve() does not clear it first
   *   (the caller reuses one scratch vector across tiles). Kept small so the row-to-slot scratch stays L1-resident.
   * @pre Run per tile in the merge phase, single-threaded on this worker's map.
   * @pre `slots_out` is empty on entry; the caller clears it (retaining capacity) before each tile, so its appended
   *   ids stay positional -- slots_out[i] is the slot for row i, the contract fold() relies on.
   * @post A dense slot and its accumulator state exist for every appended id, so a following fold() over those slots
   *   never allocates.
   * @note Establishes the resolve-before-fold ordering: fold() for this tile must be called with the slots produced
   *   here.
   * Complexity: O(rows) expected; a first-seen key may trigger one grow_index() that re-probes all live keys.
   */
  void resolve(std::span<const std::byte> key_tile, std::vector<uint32_t>& slots_out);

  /**
   * Fold one aggregate's value tile into the dense slots resolve() produced for the same tile.
   *
   * A thin forwarder to the opaque accumulator column, so the per-value typed loop lives in the column and the virtual
   * dispatch is amortized once per tile rather than per row.
   *
   * @param aggregate_index    Index of the aggregate (and its accumulator column) to fold, in aggregate order.
   * @param slots              Dense slots from the matching resolve() call; slots[i] is the slot for row i.
   * @param value_bytes        This aggregate's value stream over the same tile, raw bytes reinterpreted by the column
   *   as its input type; empty for COUNT(*), which counts every row.
   * @param value_null_bitmap  The tile's value-null-bitmap (one bit per row); empty when this aggregate's stream is
   *   non-nullable. Rows whose null bit is set are skipped.
   * @pre resolve() for this tile must have run first and `slots` must be its output (resolve-before-fold).
   */
  void fold(size_t aggregate_index, std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap);

  /** @return The number of distinct keys resolved so far, i.e. the dense slot count. */
  size_t size() const;

  /**
   * Merge every group of another map into this one, used by the low-cardinality path to reduce the per-worker
   * private maps into a single result. Each of `other`'s dense keys is resolved into this map (creating a slot if new,
   * matching an existing group otherwise) and its accumulator state is combined into the matching slot via
   * AbstractAccumulatorColumn::combine_from. The pre-finalize state is merged, so AVG and the counted aggregates stay
   * correct.
   */
  void combine(const MergeMap& other);

  /**
   * Drop all logical state while retaining capacity, readying the map for the next partition this worker claims.
   *
   * Zeroes the probe index, clears the dense keys and every accumulator column, and clears the spill buffer; allocated
   * capacity is kept so reuse avoids reallocation.
   *
   * @post size() == 0; the probe index, dense storage, and spill buffer retain their capacity.
   */
  void clear();

  /**
   * Emit this partition's grouped results as one contiguous run of output rows.
   *
   * Unpacks each dense key into the group-by output columns and finalizes each accumulator column into its aggregate
   * output column, appending exactly one value (a NULL included) to every column per emitted row.
   *
   * @param output  The owning worker's thread-local output buffers, appended to; this worker is their only writer.
   * @pre Call once per partition at flush time, after every tile of that partition has been resolve()d and fold()ed.
   * @post `output` has gained one row per distinct key (size() rows); this map's own state is unchanged (const).
   * Complexity: O(size()) rows, each an unpack plus one finalize per aggregate.
   */
  void flush_into(OutputColumns& output) const;

 private:
  static constexpr size_t MIN_TABLE_SIZE = 64;

  /** Build a fresh probe index of `table_size` entries over the live dense keys. */
  static std::vector<uint32_t> build_probe_table(size_t table_size, const std::vector<std::byte>& keys,
                                                 const KeySchema& schema, uint32_t shift);

  /** Double the probe index and re-probe every live dense key; dense indices and accumulator state are untouched. */
  void grow_index();

  std::vector<uint32_t> _table;  // probe index; entry == dense slot + 1, 0 == empty (see class comment)
  size_t _mask{0};               // _table.size() - 1; index mask for the power-of-two probe table
  size_t _max_load{0};           // grow the index once the live-key count would exceed this load threshold
  uint32_t _shift{0};            // log2(P); the probe ignores the low `shift` hash bits (constant within a partition)
  std::vector<std::byte> _keys;  // dense key bytes, stride KeySchema::packed_width()
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> _columns;  // one accumulator per aggregate, dense by slot
  StringSpillBuffer _spill;               // merge-side interned spill content; unused for numeric schemas
  const KeySchema* _key_schema{nullptr};  // borrowed; must outlive this map
};

template <typename KeySchema>
std::vector<uint32_t> MergeMap<KeySchema>::build_probe_table(const size_t table_size,
                                                             const std::vector<std::byte>& keys,
                                                             const KeySchema& schema, const uint32_t shift) {
  auto table = std::vector<uint32_t>(table_size, 0);
  const auto mask = table_size - 1;
  const auto width = schema.packed_width();
  const auto key_count = keys.size() / width;
  for (auto slot = size_t{0}; slot < key_count; ++slot) {
    auto position = (schema.hash(keys.data() + slot * width) >> shift) & mask;
    while (table[position] != 0) {
      position = (position + 1) & mask;
    }
    table[position] = static_cast<uint32_t>(slot + 1);
  }
  return table;
}

template <typename KeySchema>
MergeMap<KeySchema>::MergeMap(const KeySchema& key_schema, const uint32_t shift,
                              std::vector<std::unique_ptr<AbstractAccumulatorColumn>> columns)
    : _shift{shift}, _columns{std::move(columns)}, _key_schema{&key_schema} {}

template <typename KeySchema>
void MergeMap<KeySchema>::reserve(const size_t distinct_keys) {
  const auto table_size = std::bit_ceil(std::max(2 * distinct_keys, MIN_TABLE_SIZE));
  if (table_size > _table.size()) {
    _table = build_probe_table(table_size, _keys, *_key_schema, _shift);
    _mask = table_size - 1;
    _max_load = table_size / 2;
  }
  _keys.reserve(distinct_keys * _key_schema->packed_width());
}

template <typename KeySchema>
void MergeMap<KeySchema>::resolve(const std::span<const std::byte> key_tile, std::vector<uint32_t>& slots_out) {
  const auto width = _key_schema->packed_width();
  DebugAssert(!_table.empty(), "reserve() must run before resolve().");
  DebugAssert(key_tile.size() % width == 0, "Key tile must hold whole keys.");
  DebugAssert(key_tile.size() / width <= merge_tile_rows(), "Key tile exceeds merge_tile_rows().");
  DebugAssert(slots_out.empty(), "slots_out must be cleared by the caller.");

  const auto row_count = key_tile.size() / width;
  for (auto row = size_t{0}; row < row_count; ++row) {
    const auto* key = key_tile.data() + row * width;
    auto position = (_key_schema->hash(key) >> _shift) & _mask;
    while (true) {
      const auto entry = _table[position];
      if (entry == 0) {
        DebugAssert(size() < std::numeric_limits<uint32_t>::max(), "Dense slot ids exceed the 32-bit probe entries.");
        const auto slot = static_cast<uint32_t>(size());
        _keys.insert(_keys.end(), key, key + width);
        if constexpr (KeySchema::HAS_STRINGS) {
          _key_schema->reintern_spill(_keys.data() + size_t{slot} * width, _spill);
        }
        _table[position] = slot + 1;
        slots_out.emplace_back(slot);
        if (size() > _max_load) {
          grow_index();
        }
        break;
      }
      const auto slot = entry - 1;
      if (_key_schema->equals(key, _keys.data() + size_t{slot} * width)) {
        slots_out.emplace_back(slot);
        break;
      }
      position = (position + 1) & _mask;
    }
  }

  const auto slot_count = size();
  for (const auto& column : _columns) {
    column->grow_to(slot_count);
  }
}

template <typename KeySchema>
void MergeMap<KeySchema>::fold(const size_t aggregate_index, std::span<const uint32_t> slots,
                               std::span<const std::byte> value_bytes, std::span<const std::byte> value_null_bitmap) {
  _columns[aggregate_index]->fold(slots, value_bytes, value_null_bitmap);
}

template <typename KeySchema>
size_t MergeMap<KeySchema>::size() const {
  return _keys.size() / _key_schema->packed_width();
}

template <typename KeySchema>
void MergeMap<KeySchema>::combine(const MergeMap& other) {
  DebugAssert(!_table.empty(), "reserve() must run before combine().");
  const auto width = _key_schema->packed_width();
  const auto other_slot_count = other.size();
  auto slots = std::vector<uint32_t>{};
  const auto max_tile_rows = merge_tile_rows();
  for (auto tile_start = size_t{0}; tile_start < other_slot_count; tile_start += max_tile_rows) {
    const auto tile_rows = std::min(max_tile_rows, other_slot_count - tile_start);
    slots.clear();
    resolve({other._keys.data() + tile_start * width, tile_rows * width}, slots);
    const auto column_count = _columns.size();
    for (auto index = size_t{0}; index < column_count; ++index) {
      _columns[index]->combine_from(*other._columns[index], tile_start, slots);
    }
  }
}

template <typename KeySchema>
void MergeMap<KeySchema>::clear() {
  std::fill(_table.begin(), _table.end(), uint32_t{0});
  _keys.clear();
  for (const auto& column : _columns) {
    column->clear();
  }
  _spill.clear();
}

template <typename KeySchema>
void MergeMap<KeySchema>::flush_into(OutputColumns& output) const {
  const auto width = _key_schema->packed_width();
  const auto slot_count = size();
  for (auto slot = size_t{0}; slot < slot_count; ++slot) {
    _key_schema->unpack(_keys.data() + slot * width, output, slot);
  }

  const auto group_by_count = _key_schema->column_count();
  const auto column_count = _columns.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    _columns[index]->finalize_into(0, slot_count, group_by_count + index, output);
  }
}

template <typename KeySchema>
void MergeMap<KeySchema>::grow_index() {
  const auto table_size = std::max(_table.size() * 2, MIN_TABLE_SIZE);
  _table = build_probe_table(table_size, _keys, *_key_schema, _shift);
  _mask = table_size - 1;
  _max_load = table_size / 2;
}

}  // namespace hyrise
