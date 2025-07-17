#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "RowIdIterator.h"

namespace hyrise {

enum class NormalizedSortMode : uint8_t { Ascending, Descending };

enum class NullsMode : uint8_t { NullsFirst, NullsLast };

/**
 * @brief Creates a binary-sortable key for one or more values.
 *
 * This class implements the key normalization technique described in the DuckDB paper
 * "These Rows Are Made for Sorting". It takes values of different types and appends them
 * to a byte buffer in a specific, "normalized" format. The resulting byte array has a
 * crucial property: when two such keys are compared using `memcmp`, the result reflects
 * the desired sort order of the original values (respecting ASC/DESC and NULLS FIRST/LAST).
 * This avoids expensive, type-aware comparison logic within the hot loop of a sort algorithm.
 */
class KeyNormalizer {
 public:
  explicit KeyNormalizer(std::vector<unsigned char>& buffer);

  void append(const std::optional<int32_t>& value, NormalizedSortMode desc, NullsMode nulls_first);

  template <typename T>
  static void insert(std::vector<unsigned char>& buffer, T value, uint64_t offset, NormalizedSortMode sort_mode,
                     uint32_t string_prefix_length);

  void append(const std::optional<int64_t>& value, NormalizedSortMode desc, NullsMode nulls_first);

  void append(const std::optional<float>& value, NormalizedSortMode desc, NullsMode nulls_first);

  void append(const std::optional<double>& value, NormalizedSortMode desc, NullsMode nulls_first);

  void append(const std::optional<pmr_string>& value, NormalizedSortMode desc, NullsMode nulls_first,
              size_t prefix_size = 12);

  void append_row_id(uint64_t row_id);

  static void insert_row_id(std::vector<unsigned char>& buffer, RowID row_id, uint64_t offset);

  static void insert_null_prefix(std::vector<unsigned char>& buffer, bool is_null, uint64_t offset,
                                 NullsMode nulls_mode);

  void append_chunk(const std::shared_ptr<const Chunk>& chunk,
                    const std::vector<SortColumnDefinition>& sort_definitions);

  static void insert_chunk(std::vector<unsigned char>& buffer, const std::shared_ptr<const Chunk>& chunk,
                           const std::vector<SortColumnDefinition>& sort_definitions, uint64_t buffer_offset,
                           ChunkID chunk_id, uint32_t tuple_key_size, uint32_t string_prefix_length,
                           ChunkOffset chunk_size);

  void append_table(const std::shared_ptr<const Table>& table,
                    const std::vector<SortColumnDefinition>& sort_definitions);

  static std::pair<std::vector<unsigned char>, uint64_t> convert_table(
      const std::shared_ptr<const Table>& table, const std::vector<SortColumnDefinition>& sort_definitions,
      uint32_t string_prefix_length = 12);

  static RowIdIteratorWithEnd get_iterators(const std::vector<unsigned char>& buffer, uint64_t tuple_key_size);

 private:
  std::vector<unsigned char>& _buffer;

  /**
     * @brief Appends a single byte prefix to handle NULL ordering.
     * This byte ensures that NULLs are sorted correctly before or after non-NULL values,
     * regardless of the ASC/DESC sort order of the values themselves.
     */
  void _append_null_prefix(bool has_value, NullsMode nulls_first) const;

  template <typename T>
  void _append_integral(const std::optional<T>& value, NormalizedSortMode desc, NullsMode nulls_first);

  template <class T>
    requires std::is_integral_v<T>
  static void _insert_integral(std::vector<unsigned char>& buffer, T value, uint64_t offset,
                               NormalizedSortMode sort_mode);

  template <typename T>
  void _append_floating_point(const std::optional<T>& value, NormalizedSortMode desc, NullsMode nulls_first);

  template <class T>
    requires std::is_floating_point_v<T>
  static void _insert_floating_point(std::vector<unsigned char>& buffer, T value, uint64_t offset,
                                     NormalizedSortMode sort_mode);

  static void _insert_string(std::vector<unsigned char>& buffer, pmr_string value, uint64_t offset,
                             NormalizedSortMode sort_mode, uint32_t string_prefix_length);
};
}  // namespace hyrise
