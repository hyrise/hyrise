#pragma once

#include <cstdint>
#include <cstring>
#include <memory>
#include <memory_resource>
#include <type_traits>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

#include "operators/aggregate_dyod_utils/concurrent_ticket_map.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/chunk.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/segment_iterate.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// Target number of groups per output chunk. The grouped output columns are split into chunks of this size.
constexpr auto TARGET_CHUNK_SIZE = Chunk::DEFAULT_SIZE;

// The callbacks below have the signature `void(const pmr_string& value, const bool is_null, NeedsCopy needs_copy)`.
// `NeedsCopy` is `std::true_type` if the string value is transient and `std::false_type` if not.
template <typename ColumnDataType, typename Functor>
bool _with_string_segment_iterate_generic(const std::shared_ptr<AbstractSegment>& segment, const Functor& callback) {
  segment_iterate<ColumnDataType>(*segment, [&](const auto& position) {
    callback(position.value(), position.is_null(), std::true_type{});
  });

  return true;
}

template <typename Functor>
bool _with_string_segment_iterate(const std::shared_ptr<ValueSegment<pmr_string>>& segment, const Functor& callback) {
  const auto& values = segment->values();
  const auto value_count = values.size();

  if (segment->is_nullable()) {
    const auto& null_values = segment->null_values();
    for (auto offset = size_t{0}; offset < value_count; ++offset) {
      callback(values[offset], static_cast<bool>(null_values[offset]), std::false_type{});
    }
  } else {
    for (const auto& value : values) {
      callback(value, false, std::false_type{});
    }
  }

  return false;
}

template <typename Functor>
bool _with_string_segment_iterate(const std::shared_ptr<DictionarySegment<pmr_string>>& segment,
                                  const Functor& callback) {
  const auto& dictionary = *segment->dictionary();
  const auto null_value_id = segment->null_value_id();
  const auto placeholder_string = pmr_string{};

  resolve_compressed_vector_type(*segment->attribute_vector(), [&](const auto& attribute_vector) {
    auto decompressor = attribute_vector.create_decompressor();
    const auto value_count = attribute_vector.size();

    for (auto offset = size_t{0}; offset < value_count; ++offset) {
      const auto value_id = static_cast<ValueID>(decompressor.get(offset));

      if (value_id == null_value_id) {
        callback(placeholder_string, true, std::false_type{});
      } else {
        callback(dictionary[value_id], false, std::false_type{});
      }
    }
  });

  return false;
}

template <typename Functor>
bool _with_string_segment_iterate(const std::shared_ptr<ReferenceSegment>& segment, const Functor& callback) {
  const auto& pos_list = segment->pos_list();

  if (pos_list->empty()) {
    return false;
  }

  if (!pos_list->references_single_chunk()) {
    return _with_string_segment_iterate_generic<pmr_string>(segment, callback);
  }

  const auto& referenced_table = segment->referenced_table();
  const auto referenced_column_id = segment->referenced_column_id();
  const auto referenced_segment =
      referenced_table->get_chunk(pos_list->common_chunk_id())->get_segment(referenced_column_id);
  const auto pos_list_size = pos_list->size();

  if (const auto referenced_value = std::dynamic_pointer_cast<ValueSegment<pmr_string>>(referenced_segment)) {
    const auto& values = referenced_value->values();
    if (referenced_value->is_nullable()) {
      const auto& null_values = referenced_value->null_values();
      for (auto offset = size_t{0}; offset < pos_list_size; ++offset) {
        const auto chunk_offset = (*pos_list)[offset].chunk_offset;
        callback(values[chunk_offset], static_cast<bool>(null_values[chunk_offset]), std::false_type{});
      }
    } else {
      for (auto offset = size_t{0}; offset < pos_list_size; ++offset) {
        const auto chunk_offset = (*pos_list)[offset].chunk_offset;
        callback(values[chunk_offset], false, std::false_type{});
      }
    }

    return false;
  }

  if (const auto referenced_dictionary = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(referenced_segment)) {
    const auto& dictionary = *referenced_dictionary->dictionary();
    const auto null_value_id = referenced_dictionary->null_value_id();
    const auto placeholder_string = pmr_string{};

    resolve_compressed_vector_type(*referenced_dictionary->attribute_vector(), [&](const auto& attribute_vector) {
      auto decompressor = attribute_vector.create_decompressor();

      for (auto offset = size_t{0}; offset < pos_list_size; ++offset) {
        const auto chunk_offset = (*pos_list)[offset].chunk_offset;
        const auto value_id = static_cast<ValueID>(decompressor.get(chunk_offset));

        if (value_id == null_value_id) {
          callback(placeholder_string, true, std::false_type{});
        } else {
          callback(dictionary[value_id], false, std::false_type{});
        }
      }
    });

    return false;
  }

  // Fallback to the generic iterator for other referenced segment types.
  return _with_string_segment_iterate_generic<pmr_string>(segment, callback);
}

// TODO(@Rob2U): We should write a specialization for FixedStringDictionarySegment. Its dictionary hands out temporaries
// rather than stable `pmr_string`s, so it would need a path of its own instead of the generic fallback.
// Iterates the string values of `segment` and returns whether the values handed to `callback` had to be copied.
template <typename ColumnDataType, typename Functor>
bool with_string_segment_iterate(const std::shared_ptr<AbstractSegment>& segment, const Functor& callback) {
  if constexpr (!std::is_same_v<ColumnDataType, pmr_string>) {
    return _with_string_segment_iterate_generic<ColumnDataType>(segment, callback);
  } else {
    if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<pmr_string>>(segment)) {
      return _with_string_segment_iterate(value_segment, callback);
    }

    if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment)) {
      return _with_string_segment_iterate(dictionary_segment, callback);
    }

    if (const auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(segment)) {
      return _with_string_segment_iterate(reference_segment, callback);
    }

    return _with_string_segment_iterate_generic<pmr_string>(segment, callback);
  }
}

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

  explicit GroupKeyData(const RowFormat& _row_format, size_t estimated_groups) : GroupKeyDataBase(_row_format) {
    if constexpr (Concurrent) {
      global_hash_table.initialize(estimated_groups, GroupKeyHash{}, GroupKeyEqual{&this->row_format});
    } else {
      global_hash_table = HashTableType(estimated_groups, GroupKeyHash{}, GroupKeyEqual{&this->row_format});
    }
  }
};

// Determines the distinct groups. The returned hash table and key-row arena outlive this call.
template <bool Concurrent>
std::shared_ptr<GroupKeyData<Concurrent>> _compute_groups(const std::vector<ColumnID>& groupby_column_ids,
                                                          const std::shared_ptr<const Table>& input_table);

}  // namespace hyrise
