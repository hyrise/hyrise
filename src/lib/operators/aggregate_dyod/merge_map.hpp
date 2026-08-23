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
#include "operators/aggregate_dyod/key_primitives.hpp"
#include "operators/aggregate_dyod/output_columns.hpp"
#include "utils/assert.hpp"

namespace hyrise {

/**
 * A dense, structure-of-arrays hash accumulator that merges one radix partition's scattered rows into grouped
 * aggregates.
 *
 * This is the core of the merge phase. A merge worker streams every scatter-store row belonging to one partition
 * through this map in merge_tile_rows() tiles: resolve() maps each packed key to a dense slot, fold() folds the row's
 * values into that slot's accumulators, and flush_into() finalizes the groups into the worker's thread-local
 * OutputColumns. The dense slot id is the shared index across three parallel arrays, so grouping happens once in
 * resolve and every aggregate folds against the same slots.
 *
 * Internal representation (dense slot id `d` addresses all three):
 *   _table: open-addressing probe index: stores (d + 1), or 0 for empty. Length is a power of two. Probed by the key
 *           hash's bits above the log2(P) partition bits (the low bits are constant within a partition), linear
 *           thereafter.
 *   _keys: dense storage of the packed key bytes, `d -> key`, stride = KeySchema::packed_width().
 *   _columns: one AbstractAccumulatorColumn per aggregate, each dense `d -> accumulator`.
 */
template <typename KeySchema>
class MergeMap : private Noncopyable {
 public:
  MergeMap(const KeySchema& key_schema, uint32_t shift,
           std::vector<std::unique_ptr<AbstractAccumulatorColumn>> columns);

  void reserve(size_t distinct_keys);

  /**
   * Resolve a tile of packed keys to their dense slots, creating slots for keys not yet seen in this fill.
   *
   * Each key is hashed and probed. A previously unseen key is assigned the next dense index, its bytes copied into the
   * dense key storage, and every accumulator column grown and identity-seeded for the new slot. For a spilled string
   * key, the content is interned into this map's merge-side spill buffer on that first insertion and the stored key's
   * spill pointer repointed there, so later deep-compares read cache-resident bytes.
   */
  void resolve(std::span<const std::byte> key_tile, std::vector<uint32_t>& slots_out);

  /**
   * Fold one aggregate's value tile into the dense slots resolve() produced for the same tile.
   */
  void fold(size_t aggregate_index, std::span<const uint32_t> slots, std::span<const std::byte> value_bytes,
            std::span<const std::byte> value_null_bitmap);

  /**
   * The number of distinct keys resolved so far (the dense slot count).
   */
  size_t size() const;

  /**
   * Merge every group of another map into this one, used by the low-cardinality path to reduce the per-worker
   * private maps into a single result. Each of `other`'s dense keys is resolved into this map (creating a slot if new,
   * matching an existing group otherwise) and its accumulator state is combined into the matching slot via
   * AbstractAccumulatorColumn::combine_from.
   */
  void combine(const MergeMap& other);

  void clear();

  /**
   * Emit this partition's grouped results as one contiguous run of output rows.
   *
   * Unpacks each dense key into the group-by output columns and finalizes each accumulator column into its aggregate
   * output column, appending exactly one value (a NULL included) to every column per emitted row.
   */
  void flush_into(OutputColumns& output) const;

 private:
  static constexpr size_t MIN_TABLE_SIZE = 64;

  static std::vector<uint32_t> _build_probe_table(size_t table_size, const std::vector<std::byte>& keys,
                                                  const KeySchema& schema, uint32_t shift);

  void _grow_index();

  std::vector<uint32_t> _table;
  size_t _mask{0};
  size_t _max_load{0};
  uint32_t _shift{0};
  std::vector<std::byte> _keys;
  std::vector<std::unique_ptr<AbstractAccumulatorColumn>> _columns;
  StringSpillBuffer _spill;
  const KeySchema* _key_schema{nullptr};
};

template <typename KeySchema>
std::vector<uint32_t> MergeMap<KeySchema>::_build_probe_table(const size_t table_size,
                                                              const std::vector<std::byte>& keys,
                                                              const KeySchema& schema, const uint32_t shift) {
  auto table = std::vector<uint32_t>(table_size, 0);
  const auto mask = table_size - 1;
  const auto width = schema.packed_width();
  const auto key_count = keys.size() / width;
  for (auto slot = size_t{0}; slot < key_count; ++slot) {
    auto position = (schema.hash(keys.data() + (slot * width)) >> shift) & mask;
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
    _table = _build_probe_table(table_size, _keys, *_key_schema, _shift);
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
    const auto* key = key_tile.data() + (row * width);
    auto position = (_key_schema->hash(key) >> _shift) & _mask;
    while (true) {
      const auto entry = _table[position];
      if (entry == 0) {
        DebugAssert(size() < std::numeric_limits<uint32_t>::max(), "Dense slot ids exceed the 32-bit probe entries.");
        const auto slot = static_cast<uint32_t>(size());
        _keys.insert(_keys.end(), key, key + width);
        if constexpr (KeySchema::HAS_STRINGS) {
          _key_schema->reintern_spill(_keys.data() + (size_t{slot} * width), _spill);
        }
        _table[position] = slot + 1;
        slots_out.emplace_back(slot);
        if (size() > _max_load) {
          _grow_index();
        }
        break;
      }
      const auto slot = entry - 1;
      if (_key_schema->equals(key, _keys.data() + (size_t{slot} * width))) {
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
    resolve({other._keys.data() + (tile_start * width), tile_rows * width}, slots);
    const auto column_count = _columns.size();
    for (auto index = size_t{0}; index < column_count; ++index) {
      _columns[index]->combine_from(*other._columns[index], tile_start, slots);
    }
  }
}

template <typename KeySchema>
void MergeMap<KeySchema>::clear() {
  std::ranges::fill(_table, uint32_t{0});
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
    _key_schema->unpack(_keys.data() + (slot * width), output, slot);
  }

  const auto group_by_count = _key_schema->column_count();
  const auto column_count = _columns.size();
  for (auto index = size_t{0}; index < column_count; ++index) {
    _columns[index]->finalize_into(0, slot_count, group_by_count + index, output);
  }
}

template <typename KeySchema>
void MergeMap<KeySchema>::_grow_index() {
  const auto table_size = std::max(_table.size() * 2, MIN_TABLE_SIZE);
  _table = _build_probe_table(table_size, _keys, *_key_schema, _shift);
  _mask = table_size - 1;
  _max_load = table_size / 2;
}

}  // namespace hyrise
