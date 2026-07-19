#include "operators/aggregate_dyod/merge_map.hpp"

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

namespace {

using namespace hyrise;

constexpr size_t MIN_TABLE_SIZE = 64;

template <typename KeySchema>
std::vector<uint32_t> build_probe_table(const size_t table_size, const std::vector<std::byte>& keys,
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

}  // namespace

namespace hyrise {

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
  DebugAssert(key_tile.size() / width <= MERGE_TILE_ROWS, "Key tile exceeds MERGE_TILE_ROWS.");
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

template class MergeMap<NumericShortKeySchema<4>>;
template class MergeMap<NumericShortKeySchema<8>>;
template class MergeMap<NumericShortKeySchema<12>>;
template class MergeMap<NumericShortKeySchema<16>>;
template class MergeMap<NumericArbitraryKeySchema>;
template class MergeMap<MixedKeySchema<4>>;
template class MergeMap<StringOnlyKeySchema<4>>;

}  // namespace hyrise
