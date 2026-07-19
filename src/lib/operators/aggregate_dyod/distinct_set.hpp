#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>
#include <type_traits>
#include <vector>

#include "operators/aggregate_dyod/key_schema.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * Membership set over (slot, value) pairs backing COUNT(DISTINCT): insert() reports whether a value is a slot's
 * first sighting, and the caller counts those sightings.
 *
 * Equality is value equality on the canonical form: -0.0 and +0.0 collapse and every NaN counts as one distinct
 * value (the same canonicalization the key side applies, see key_schema.hpp); strings compare by content and are
 * interned into an owned StringSpillBuffer on first sighting, so callers may pass transient views. Probing works
 * like the MergeMap: entry-index-plus-one probe table, linear probing, grown at half load.
 */
template <typename ColumnType>
class DistinctSet : private Noncopyable {
 public:
  // Strings are passed as borrowed views; the content is copied on first sighting.
  using ValueView = std::conditional_t<std::is_same_v<ColumnType, pmr_string>, std::string_view, ColumnType>;

  /** @return true iff no equal value was recorded for this slot since the last clear(). */
  bool insert(uint32_t slot, ValueView value);

  /** Union another set's entries into this one, re-interning string content. */
  void merge(const DistinctSet& other);

  size_t size() const;

  /** Drop all entries but keep allocated capacity. */
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

  uint64_t _entry_hash(const Entry& entry) const;
  void _grow_table();
  void _grow_table_to(size_t table_size);

  std::vector<uint32_t> _table;  // probe index: entry index + 1, or 0 for empty; length is a power of two
  size_t _mask{0};               // _table.size() - 1
  size_t _max_load{0};           // grow the index once the entry count would exceed this load threshold
  std::vector<Entry> _entries;   // dense entry storage; an entry's index, once assigned, is stable until clear()
  StringSpillBuffer _content;    // interned string payloads (string instantiation only)
};

}  // namespace hyrise
