#include "operators/aggregate_dyod/distinct_set.hpp"

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string_view>
#include <type_traits>

#include "operators/aggregate_dyod/key_schema.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;

constexpr auto MIN_TABLE_SIZE = size_t{64};

// Continues the value's hash over the slot, so equal values in different slots probe different chains.
uint64_t combine_slot(const uint64_t value_hash, const uint32_t slot) {
  return hash_bytes(reinterpret_cast<const std::byte*>(&slot), sizeof(slot), value_hash);
}

uint32_t canonical_bits(const int32_t value) {
  return std::bit_cast<uint32_t>(value);
}

uint64_t canonical_bits(const int64_t value) {
  return std::bit_cast<uint64_t>(value);
}

uint32_t canonical_bits(const float value) {
  return std::bit_cast<uint32_t>(canonicalize(value));
}

uint64_t canonical_bits(const double value) {
  return std::bit_cast<uint64_t>(canonicalize(value));
}

}  // namespace

namespace hyrise {

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

  auto index = _entry_hash(entry) & _mask;
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
    auto index = _entry_hash(_entries[entry_index]) & _mask;
    while (_table[index] != 0) {
      index = (index + 1) & _mask;
    }
    _table[index] = static_cast<uint32_t>(entry_index + 1);
  }
}

template class DistinctSet<int32_t>;
template class DistinctSet<int64_t>;
template class DistinctSet<float>;
template class DistinctSet<double>;
template class DistinctSet<pmr_string>;

}  // namespace hyrise
