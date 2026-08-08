#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>
#include <vector>

#include "operators/aggregate_dyod/hyperloglog.hpp"
#include "operators/aggregate_dyod/key_schema.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Continues the value's hash over the slot, so equal values in different slots probe different chains.
inline uint64_t combine_slot(const uint64_t value_hash, const uint32_t slot) {
  return hash_bytes(reinterpret_cast<const std::byte*>(&slot), sizeof(slot), value_hash);
}

inline uint32_t canonical_bits(const int32_t value) {
  return std::bit_cast<uint32_t>(value);
}

inline uint64_t canonical_bits(const int64_t value) {
  return std::bit_cast<uint64_t>(value);
}

inline uint32_t canonical_bits(const float value) {
  return std::bit_cast<uint32_t>(canonicalize(value));
}

inline uint64_t canonical_bits(const double value) {
  return std::bit_cast<uint64_t>(canonicalize(value));
}

/**
 * Membership set over (slot, value) pairs for COUNT(DISTINCT): insert() reports whether a value is a slot's
 * first sighting, and the caller counts those sightings.
 */
template <typename ColumnType>
class DistinctSet : private Noncopyable {
 public:
  // Strings are passed as borrowed views; the content is copied on first sighting.
  using ValueView = std::conditional_t<std::is_same_v<ColumnType, pmr_string>, std::string_view, ColumnType>;

  /**
   * returns true iff no equal value was recorded for this slot since the last clear().
   */
  bool insert(uint32_t slot, ValueView value);

  void merge(const DistinctSet& other);

  /**
   * Distribute the entries into `targets` by hash range. Equal (slot, value) pairs land in the same target
   * regardless of which set they came from, so per-range unions across many sets partition the distinct count.
   */
  void split_into(std::vector<DistinctSet>& targets) const;

  size_t size() const;

  void clear();

 private:
  struct NumericEntry {
    uint32_t slot{0};
    std::conditional_t<sizeof(ColumnType) == 4, uint32_t, uint64_t> bits{0};
  };

  struct StringEntry {
    uint32_t slot{0};
    uint64_t content_hash{0};
    const std::byte* data{nullptr};
    uint64_t length{0};
  };

  using Entry = std::conditional_t<std::is_same_v<ColumnType, pmr_string>, StringEntry, NumericEntry>;

  static constexpr auto MIN_TABLE_SIZE = size_t{64};

  uint64_t _entry_hash(const Entry& entry) const;
  void _grow_table();
  void _grow_table_to(size_t table_size);

  std::vector<uint32_t> _table;  // probe index: entry index + 1, or 0 for empty; length is a power of two
  size_t _mask{0};               // _table.size() - 1
  size_t _max_load{0};           // grow the index once the entry count would exceed this load threshold
  std::vector<Entry> _entries;   // dense entry storage; an entry's index, once assigned, is stable until clear()
  StringSpillBuffer _content;    // interned string payloads (string instantiation only)
};

template <typename ColumnType>
bool DistinctSet<ColumnType>::insert(const uint32_t slot, const ValueView value) {
  if (_table.empty()) {
    _grow_table();
  }

  auto entry = Entry{};
  entry.slot = slot;
  if constexpr (std::is_same_v<ColumnType, pmr_string>) {
    entry.content_hash = hash_bytes(reinterpret_cast<const std::byte*>(value.data()), value.size());
    entry.length = value.size();
  } else {
    entry.bits = canonical_bits(value);
  }

  // FNV-1a's low bits carry input structure almost unchanged; mixing keeps structured keys from clustering.
  auto index = mix64(_entry_hash(entry)) & _mask;
  while (true) {
    const auto stored = _table[index];
    if (stored == 0) {
      DebugAssert(_entries.size() < std::numeric_limits<uint32_t>::max(),
                  "Entry indices exceed the 32-bit probe entries.");
      if constexpr (std::is_same_v<ColumnType, pmr_string>) {
        entry.data = _content.append(reinterpret_cast<const std::byte*>(value.data()), value.size());
      }
      _entries.emplace_back(entry);
      _table[index] = static_cast<uint32_t>(_entries.size());
      if (_entries.size() > _max_load) {
        _grow_table();
      }
      return true;
    }

    const auto& existing = _entries[stored - 1];
    if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      if (existing.slot == slot && existing.content_hash == entry.content_hash &&
          std::string_view{reinterpret_cast<const char*>(existing.data), existing.length} == value) {
        return false;
      }
    } else {
      if (existing.slot == slot && existing.bits == entry.bits) {
        return false;
      }
    }
    index = (index + 1) & _mask;
  }
}

template <typename ColumnType>
void DistinctSet<ColumnType>::merge(const DistinctSet& other) {
  const auto expected_entries = _entries.size() + other._entries.size();
  const auto table_size = std::bit_ceil(std::max(2 * expected_entries, MIN_TABLE_SIZE));
  if (table_size > _table.size()) {
    _grow_table_to(table_size);
  }
  _entries.reserve(expected_entries);
  for (const auto& entry : other._entries) {
    if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      insert(entry.slot, std::string_view{reinterpret_cast<const char*>(entry.data), entry.length});
    } else {
      insert(entry.slot, std::bit_cast<ColumnType>(entry.bits));
    }
  }
}

template <typename ColumnType>
void DistinctSet<ColumnType>::split_into(std::vector<DistinctSet>& targets) const {
  const auto target_count = targets.size();
  Assert(std::has_single_bit(target_count), "Split requires a power-of-two target count.");
  if (target_count == 1) {
    targets.front().merge(*this);
    return;
  }

  const auto shift = 64 - std::countr_zero(target_count);
  for (const auto& entry : _entries) {
    auto& target = targets[mix64(_entry_hash(entry)) >> shift];
    if constexpr (std::is_same_v<ColumnType, pmr_string>) {
      target.insert(entry.slot, std::string_view{reinterpret_cast<const char*>(entry.data), entry.length});
    } else {
      target.insert(entry.slot, std::bit_cast<ColumnType>(entry.bits));
    }
  }
}

template <typename ColumnType>
size_t DistinctSet<ColumnType>::size() const {
  return _entries.size();
}

template <typename ColumnType>
void DistinctSet<ColumnType>::clear() {
  _entries.clear();
  std::fill(_table.begin(), _table.end(), uint32_t{0});
  _content.clear();
}

template <typename ColumnType>
uint64_t DistinctSet<ColumnType>::_entry_hash(const Entry& entry) const {
  if constexpr (std::is_same_v<ColumnType, pmr_string>) {
    return combine_slot(entry.content_hash, entry.slot);
  } else {
    const auto value_hash = hash_bytes(reinterpret_cast<const std::byte*>(&entry.bits), sizeof(entry.bits));
    return combine_slot(value_hash, entry.slot);
  }
}

template <typename ColumnType>
void DistinctSet<ColumnType>::_grow_table() {
  _grow_table_to(std::max(_table.size() * 2, MIN_TABLE_SIZE));
}

template <typename ColumnType>
void DistinctSet<ColumnType>::_grow_table_to(const size_t table_size) {
  _table.assign(table_size, 0);
  _mask = table_size - 1;
  _max_load = table_size / 2;

  const auto entry_count = _entries.size();
  for (auto entry_index = size_t{0}; entry_index < entry_count; ++entry_index) {
    auto index = mix64(_entry_hash(_entries[entry_index])) & _mask;
    while (_table[index] != 0) {
      index = (index + 1) & _mask;
    }
    _table[index] = static_cast<uint32_t>(entry_index + 1);
  }
}

}  // namespace hyrise
