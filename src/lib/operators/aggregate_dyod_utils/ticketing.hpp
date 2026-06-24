#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "storage/chunk.hpp"

namespace hyrise {

// Number of leading string bytes stored inline in a row. Strings up to this length live entirely inline
// (length + prefix); longer strings additionally store a heap pointer to the full value at the end of the row.
constexpr uint64_t PREFIX_LENGTH = 8;

// TODO(@forUnity): check if the typing (uintxx_t) makes sense here.
struct RowFormat {
  const uint64_t row_size;                                             // Size of a single row in bytes
  const uint64_t hash_offset = 0;                                      // Offset of the hash in a single row
  const uint64_t null_bitmap_offset = hash_offset + sizeof(uint64_t);  // Skip the hash
  const uint64_t data_offset = null_bitmap_offset + sizeof(uint64_t);  // Skip the hash and the null bitmap
  const uint64_t string_ptr_offset =
      data_offset + row_size;               // Offsets of the string pointers at the end of the row (if any)
  const std::vector<uint64_t> col_offsets;  // Offsets of the different columns in a single row
};

RowFormat _create_row_format(const std::shared_ptr<const Table>& input_table,
                             const std::vector<ColumnID>& groupby_column_ids);

struct MaterializedRows {
  const uint64_t row_count;
  uint8_t* rows;  // Pointer to the start of the materialized rows
  const RowFormat& format;

  MaterializedRows(const uint64_t row_count, uint8_t* rows, const RowFormat& format)
      : row_count(row_count), rows(rows), format(format) {}
  // Rule of three :(
  ~MaterializedRows();
  MaterializedRows(const MaterializedRows&) = delete;
  MaterializedRows& operator=(const MaterializedRows&) = delete;
};

std::shared_ptr<MaterializedRows> _materialize_rows(const RowFormat format, const std::shared_ptr<const Chunk>& chunk,
                                                    const std::vector<ColumnID>& groupby_column_ids);

// Methods necessary:
// row_write_col<ColumnDataType>(row, col_idx, value)
// row_read_string_ptr(row, col_idx) -> const char*, const size_t
// read_row_key?
// read_row_data?

struct GroupKey {
  const uint8_t* row;
  const RowFormat& format;  // TODO(@forUnity): this should not be stored here but rather in the groupkeydata thingy.

  // Rule of three :(
  GroupKey(const uint8_t* row, const RowFormat& format) : row(row), format(format) {}

  // Delete the row copy when the GroupKey is destroyed. The delete[] manages the size of the array automatically.
  ~GroupKey() {
    if (row != nullptr) {
      delete[] row;
    }
  }

  GroupKey(GroupKey&& other) noexcept : row(other.row), format(other.format) {
    other.row = nullptr;  // moved-from object must not delete[] our row
  };
  GroupKey& operator=(GroupKey&&) = delete;  // format is a reference, can't reassign

  GroupKey(const GroupKey&) = delete;
  GroupKey& operator=(const GroupKey&) = delete;
};

struct GroupKeyHash {
  constexpr static const auto hash_function = std::hash<std::string_view>{};

  size_t operator()(const GroupKey& key) const {
    // TODO(@forUnity): implement a real hash function here.
    // Hash exactly the bytes that GroupKeyEqual compares: the null bitmap plus the inline key data (length + prefix
    // for strings). Starting at `null_bitmap_offset` (not the row start) keeps the hash independent of the stored
    // hash field and consistent with equality. Two long strings sharing a prefix may collide, but GroupKeyEqual
    // resolves them via a full string comparison.
    return hash_function(std::string_view{reinterpret_cast<const char*>(key.row + key.format.null_bitmap_offset),
                                          key.format.string_ptr_offset - key.format.null_bitmap_offset});
  }
};

struct GroupKeyEqual {
  bool operator()(const GroupKey& lhs, const GroupKey& rhs) const {
    // DebugAssert that both formats are the same
    DebugAssert(&lhs.format == &rhs.format, "GroupKeyEqual: lhs and rhs have different formats");

    if (std::memcmp(lhs.row + lhs.format.null_bitmap_offset, rhs.row + rhs.format.null_bitmap_offset,
                    lhs.format.string_ptr_offset - lhs.format.null_bitmap_offset)) {
      return false;
    }
    // Now we have to compare the string pointers one by one :(
    const auto string_col_count = (lhs.format.row_size - lhs.format.string_ptr_offset) / sizeof(const char*);
    for (auto string_col_index = size_t{0}; string_col_index < string_col_count; ++string_col_index) {
      const auto* lhs_str_ptr = *reinterpret_cast<const char* const*>(lhs.row + lhs.format.string_ptr_offset +
                                                                      string_col_index * sizeof(const char*));
      const auto* rhs_str_ptr = *reinterpret_cast<const char* const*>(rhs.row + rhs.format.string_ptr_offset +
                                                                      string_col_index * sizeof(const char*));
      // Short strings (<= PREFIX_LENGTH) are fully represented inline and already compared by the memcmp above; they
      // carry no heap pointer (nullptr). Only long strings, which share length and prefix here, need a full compare.
      if (lhs_str_ptr == nullptr || rhs_str_ptr == nullptr) {
        continue;
      }
      if (std::strcmp(lhs_str_ptr, rhs_str_ptr)) {
        return false;
      }
    }

    return true;
  }
};

struct GroupKeyData {
  const RowFormat row_format;  // Format of the rows in `keys`
  std::unordered_map<GroupKey, uint64_t, GroupKeyHash, GroupKeyEqual>
      global_hash_table;           // Maps a group key to its index in `keys` and the output vectors
  std::vector<size_t> row_counts;  // number of rows per group (for COUNT(*))
  std::vector<uint64_t>
      tickets;  // PER ROW: ticket for this specific group key (index into `keys` and the output vectors)

  GroupKeyData(const RowFormat row_format) : row_format(row_format) {}
};

// Reads the group-by column values of `chunk_offset` into `key`.
inline void _read_group_key(const std::shared_ptr<const Chunk>& chunk, const std::vector<ColumnID>& groupby_column_ids,
                            const ChunkOffset chunk_offset, GroupKey& key);

// Determines the distinct groups. A group exists if any row maps to it, even if its aggregated values are all NULL or
// its group-by key contains NULL (NULL forms its own group).
std::shared_ptr<GroupKeyData> _compute_group_keys(const std::vector<ColumnID>& groupby_column_ids,
                                                  const std::shared_ptr<const Table>& input_table);
}  // namespace hyrise
