#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <unordered_map>
#include <vector>

#include "storage/chunk.hpp"
#include "storage/table_column_definition.hpp"
#include "utils/assert.hpp"

inline std::uint64_t rotl(std::uint64_t x, int r) {
  return (x << r) | (x >> (64 - r));
}

inline std::uint64_t read64(const unsigned char* p) {
  std::uint64_t v;
  std::memcpy(&v, p, sizeof v);  // single mov, no UB
  return v;
}

inline std::uint64_t fmix64(std::uint64_t k) {
  k ^= k >> 33;
  k *= 0xff51afd7ed558ccdULL;
  k ^= k >> 33;
  k *= 0xc4ceb9fe1a85ec53ULL;
  k ^= k >> 33;
  return k;
}

// Single-lane MurmurHash3-x64 body, now with a 1–7 byte tail.
inline std::uint64_t compute_hash(const void* key, std::size_t len, std::uint64_t seed = 0) {
  const unsigned char* p = static_cast<const unsigned char*>(key);
  const std::size_t nblocks = len / 8;

  constexpr std::uint64_t c1 = 0x87c37b91114253d5ULL;
  constexpr std::uint64_t c2 = 0x4cf5ad432745937fULL;

  std::uint64_t h = seed;

  // body: full 8-byte words
  for (std::size_t i = 0; i < nblocks; ++i) {
    std::uint64_t k = read64(p + i * 8);
    k *= c1;
    k = rotl(k, 31);
    k *= c2;
    h ^= k;
    h = rotl(h, 27);
    h = h * 5 + 0x52dce729ULL;
  }

  // tail: either nothing, or exactly 4 bytes
  if (len & 4) {
    std::uint32_t t;
    std::memcpy(&t, p + nblocks * 8, 4);  // single 32-bit load, no UB
    std::uint64_t k = t;
    k *= c1;
    k = rotl(k, 31);
    k *= c2;
    h ^= k;
    // no h = rotl/h*5+const here, matching MurmurHash3's tail
  }

  return fmix64(h);  // avalanche
}

namespace hyrise {

// Number of leading string bytes stored inline in a row. Strings up to this length live entirely inline
// (length + prefix). Longer strings additionally store a heap pointer to the full value at the end of the row.
constexpr uint64_t PREFIX_LENGTH = 8;

// Byte layout of a single materialized group-by row:
//   [hash | null bitmap? | inline column data ... | string pointers ...]
// `col_offsets` are relative to `data_offset`. String columns store `[length, prefix]` inline. Longer strings
// additionally store a heap pointer in the string-pointer area at the end of the row. The null bitmap is only present
// when at least one group-by column is nullable (`stores_nulls`); otherwise it is omitted and the rows are 8 bytes
// shorter. When it is absent, `null_bitmap_offset == data_offset`, so `key_bytes()` naturally starts at the data.
struct RowFormat {
  uint64_t row_size;                                             // Size of a single row in bytes
  uint64_t hash_offset = 0;                                      // Offset of the hash in a single row
  uint64_t null_bitmap_offset = hash_offset + sizeof(uint64_t);  // Skip the hash
  uint64_t data_offset = null_bitmap_offset + sizeof(uint64_t);  // Skip the hash and the null bitmap
  uint64_t string_ptr_offset = data_offset;                      // Offsets of the string pointers at the end of the row
  bool stores_nulls = true;                                      // Whether a null bitmap is present in each row
  std::vector<uint64_t> col_offsets;                             // Offsets of the columns relative to `data_offset`
  std::vector<uint8_t> column_is_nullable;                       // Per group-by column: 1 if nullable, else 0
};

RowFormat _create_row_format(const TableColumnDefinitions& column_definitions,
                             const std::vector<ColumnID>& groupby_column_ids);

// Non-owning view on a single materialized row. It helps interpreting the byte layout.
// Copying a `RowView` is cheap because it never owns the underlying bytes.
struct RowView {
  uint8_t* base;
  const RowFormat& format;

  uint64_t hash() const {
    return *reinterpret_cast<const uint64_t*>(base + format.hash_offset);
  }

  void set_hash() const {
    const auto value = compute_hash(key_bytes(), format.string_ptr_offset - format.null_bitmap_offset);
    *reinterpret_cast<uint64_t*>(base + format.hash_offset) = value;
  }

  uint64_t null_bitmap() const {
    DebugAssert(format.stores_nulls, "Row has no null bitmap (no group-by column is nullable).");
    return *reinterpret_cast<const uint64_t*>(base + format.null_bitmap_offset);
  }

  void set_null_bitmap(const uint64_t value) const {
    DebugAssert(format.stores_nulls, "Row has no null bitmap (no group-by column is nullable).");
    *reinterpret_cast<uint64_t*>(base + format.null_bitmap_offset) = value;
  }

  // Address of the inline data of group-by column `group_index` (index into `groupby_column_ids`).
  uint8_t* column_data(const size_t group_index) const {
    return base + format.data_offset + format.col_offsets[group_index];
  }

  template <typename T>
  T read_value(const size_t group_index) const {
    auto value = T{};
    std::memcpy(&value, column_data(group_index), sizeof(T));
    return value;
  }

  template <typename T>
  void write_value(const size_t group_index, const T& value) const {
    std::memcpy(column_data(group_index), &value, sizeof(T));
  }

  // Inline string representation: a `size_t` length followed by up to `PREFIX_LENGTH` prefix bytes.
  size_t string_length(const size_t group_index) const {
    auto length = size_t{0};
    std::memcpy(&length, column_data(group_index), sizeof(size_t));
    return length;
  }

  const char* string_prefix(const size_t group_index) const {
    return reinterpret_cast<const char*>(column_data(group_index) + sizeof(size_t));
  }

  // Heap pointer to the full value of the `string_col_index`-th string column (nullptr for short strings).
  char* string_ptr(const size_t string_col_index) const {
    return *reinterpret_cast<char**>(base + format.string_ptr_offset + string_col_index * sizeof(char*));
  }

  void set_string_ptr(const size_t string_col_index, char* const value) const {
    *reinterpret_cast<char**>(base + format.string_ptr_offset + string_col_index * sizeof(char*)) = value;
  }

  // The bytes that participate in hashing and equality: the null bitmap plus the inline key data (length + prefix
  // for strings). Independent of the stored hash and the heap string pointers.
  const uint8_t* key_bytes() const {
    return reinterpret_cast<const uint8_t*>(base + format.null_bitmap_offset);
  }

  size_t string_col_count() const {
    return (format.row_size - format.string_ptr_offset) / sizeof(char*);
  }
};

// All materialized rows of a single chunk, packed back-to-back in `rows`. Long group-by strings referenced by those
// rows live in `string_arena` and are freed together with this object once the chunk has been processed.
struct MaterializedRows {
  uint64_t row_count = 0;
  std::unique_ptr<uint8_t[]> rows;
  std::pmr::monotonic_buffer_resource string_arena;
};

std::shared_ptr<MaterializedRows> _materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                                                    const std::vector<ColumnID>& groupby_column_ids);

// Non-owning key into the global hash table. `row` points into the arena owned by `GroupKeyData`. `hash` is the
// precomputed row hash, reused on every probe instead of recomputing it.
struct GroupKey {
  uint8_t* row;
  uint64_t hash;
};

struct GroupKeyHash {
  size_t operator()(const GroupKey& key) const {
    return key.hash;
  }
};

struct GroupKeyEqual {
  const RowFormat* format;

  bool operator()(const GroupKey& lhs, const GroupKey& rhs) const {
    const auto lhs_view = RowView{lhs.row, *format};
    const auto rhs_view = RowView{rhs.row, *format};

    // Compare the null bitmap and inline key data in one shot.
    if (std::memcmp(lhs_view.key_bytes(), rhs_view.key_bytes(),
                    format->string_ptr_offset - format->null_bitmap_offset)) {
      return false;
    }

    // Long strings share length and prefix here, so they need a full comparison via their heap pointers. Short strings
    // (<= PREFIX_LENGTH) are fully represented inline and were already compared above; they carry no heap pointer.
    const auto string_col_count = lhs_view.string_col_count();
    for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
      const auto* lhs_str = lhs_view.string_ptr(string_col_index);
      const auto* rhs_str = rhs_view.string_ptr(string_col_index);
      if (lhs_str == nullptr || rhs_str == nullptr) {
        continue;
      }
      if (std::strcmp(lhs_str, rhs_str) != 0) {
        return false;
      }
    }

    return true;
  }
};

struct GroupKeyData {
  RowFormat row_format;
  // Owns the copied distinct key rows and their long strings. Is freed when this object is destroyed.
  std::pmr::monotonic_buffer_resource key_arena;

  // Maps a group key to its index in the output vectors
  std::unordered_map<GroupKey, uint64_t, GroupKeyHash, GroupKeyEqual> global_hash_table;
  std::vector<size_t> row_counts;

  // PER ROW: ticket for this specific group key (index into `keys` and the output vectors)
  std::vector<uint64_t> tickets;

  explicit GroupKeyData(const RowFormat& row_format)
      : row_format(row_format), global_hash_table(0, GroupKeyHash{}, GroupKeyEqual{&this->row_format}) {}
};

// Determines the distinct groups. A group exists if any row maps to it, even if its aggregated values are all NULL or
// its group-by key contains NULL (NULL forms its own group).
std::shared_ptr<GroupKeyData> _compute_group_keys(const std::vector<ColumnID>& groupby_column_ids,
                                                  const std::shared_ptr<const Table>& input_table);
}  // namespace hyrise
