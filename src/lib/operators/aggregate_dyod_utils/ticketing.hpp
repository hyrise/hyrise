#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "storage/chunk.hpp"
#include "storage/table_column_definition.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Target number of groups per output chunk. The grouped output columns are split into chunks of this size.
constexpr auto TARGET_CHUNK_SIZE = Chunk::DEFAULT_SIZE;

// Number of leading string bytes stored inline in a row.
constexpr uint64_t PREFIX_LENGTH = 8;

// Byte layout of a single materialized group-by row:
//   [null bitmap? | inline column data ... | string pointers ...]
// `col_offsets` are relative to `data_offset`. String columns store `[length, prefix]` inline. Longer strings
// additionally store a heap pointer in the string-pointer area at the end of the row. The null bitmap is only present
// when at least one group-by column is nullable (`stores_nulls`). Otherwise it is omitted and the rows are 8 bytes
// shorter. When it is absent, `null_bitmap_offset == data_offset`, so `key_bytes()` naturally starts at the data.
struct RowFormat {
  uint64_t row_size;
  uint64_t null_bitmap_offset = 0;
  uint64_t data_offset = null_bitmap_offset + sizeof(uint64_t);
  uint64_t string_ptr_offset = data_offset;
  uint64_t key_length = string_ptr_offset - null_bitmap_offset;
  bool stores_nulls = true;
  std::vector<uint64_t> col_offsets;
  std::vector<uint8_t> column_is_nullable;
};

RowFormat _create_row_format(const TableColumnDefinitions& column_definitions,
                             const std::vector<ColumnID>& groupby_column_ids);

struct RowView {
  uint8_t* base;
  const RowFormat& format;

  uint64_t null_bitmap() const {
    DebugAssert(format.stores_nulls, "Row has no null bitmap (no group-by column is nullable).");
    return *reinterpret_cast<const uint64_t*>(base + format.null_bitmap_offset);
  }

  void set_null_bitmap(const uint64_t value) const {
    DebugAssert(format.stores_nulls, "Row has no null bitmap (no group-by column is nullable).");
    *reinterpret_cast<uint64_t*>(base + format.null_bitmap_offset) = value;
  }

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
  // for strings).
  const uint8_t* key_bytes() const {
    return reinterpret_cast<const uint8_t*>(base + format.null_bitmap_offset);
  }

  size_t string_col_count() const {
    return (format.row_size - format.string_ptr_offset) / sizeof(char*);
  }
};

// All materialized rows of a single chunk, packed in `rows`.
struct MaterializedRows {
  uint64_t row_count = 0;
  std::unique_ptr<uint8_t[]> rows;
  std::pmr::monotonic_buffer_resource string_arena;
  // Per string group-by column (indexed by string-column order).
  std::vector<bool> string_pointer_needs_copy;
};

void _materialize_rows(const RowFormat& format, const std::shared_ptr<const Chunk>& chunk,
                       const std::vector<ColumnID>& groupby_column_ids, MaterializedRows& materialized);

// Key into the global hash table. `row` points into the arena owned by `GroupKeyData`. `hash` is the precomputed row
// hash, reused on every probe instead of recomputing it.
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
    if (std::memcmp(lhs_view.key_bytes(), rhs_view.key_bytes(), format->key_length)) {
      return false;
    }

    // Long strings need a full comparison via their heap pointers.
    const auto string_col_count = lhs_view.string_col_count();
    for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
      const auto* lhs_str = lhs_view.string_ptr(string_col_index);
      const auto* rhs_str = rhs_view.string_ptr(string_col_index);
      if (lhs_str == nullptr || rhs_str == nullptr) {
        continue;
      }
      // Fast path: identical pointers point at the same source string, so no byte comparison is needed.
      if (lhs_str == rhs_str) {
        continue;
      }
      if (std::strcmp(lhs_str, rhs_str) != 0) {
        return false;
      }
    }

    return true;
  }
};

struct GroupKeyDataBase {
  RowFormat row_format;
  std::vector<std::unique_ptr<std::pmr::monotonic_buffer_resource>> key_arenas;

  // PER ROW: the group index (ticket) of that input row.
  std::unique_ptr<uint64_t[]> tickets;
  size_t group_count = 0;

  // Whether `global_hash_table` is populated and can be read to recover each group's group-by values from its key row.
  bool has_hash_table = false;

  explicit GroupKeyDataBase(const RowFormat& _row_format) : row_format(_row_format) {}
};

template <bool Concurrent>
struct GroupKeyData : GroupKeyDataBase {
  using HashTableType = std::conditional_t<Concurrent, ConcurrentTicketMap<GroupKey, GroupKeyHash, GroupKeyEqual>,
                                           boost::unordered_flat_map<GroupKey, uint64_t, GroupKeyHash, GroupKeyEqual>>;
  HashTableType global_hash_table;

  explicit GroupKeyData(const RowFormat& _row_format) : GroupKeyDataBase(_row_format) {
    if constexpr (!Concurrent) {
      global_hash_table = HashTableType(0, GroupKeyHash{}, GroupKeyEqual{&this->row_format});
    }
  }
};

// Determines the distinct groups. The returned hash table and key-row arena outlive this call.
template <bool Concurrent>
std::shared_ptr<GroupKeyData<Concurrent>> _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                                                          const std::shared_ptr<const Table>& input_table);

}  // namespace hyrise
