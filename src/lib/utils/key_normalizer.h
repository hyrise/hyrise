#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <type_traits>
#include <vector>

namespace hyrise {

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
  explicit KeyNormalizer();

  static std::pair<std::vector<unsigned char>, uint64_t> normalize_keys_for_table(
      const std::shared_ptr<const Table>& table, const std::vector<SortColumnDefinition>& sort_definitions,
      uint32_t string_prefix_length = 8);

 private:
  static void _insert_keys_for_chunk(std::vector<unsigned char>& buffer, const std::shared_ptr<const Chunk>& chunk,
                                    const std::vector<SortColumnDefinition>& sort_definitions, uint64_t table_offset,
                                    ChunkID chunk_id, uint32_t tuple_key_size, uint32_t string_prefix_length);

  template <typename T>
  static void _insert_normalized_value(std::vector<unsigned char>& buffer, const T& value, uint64_t offset, bool descending,
                                       uint32_t string_prefix_length);

  template <typename T>
    requires std::is_integral_v<T>
  static void _insert_integral(std::vector<unsigned char>& buffer, T value, uint64_t offset, bool descending);

  template <typename T>
    requires std::is_floating_point_v<T>
  static void _insert_floating_point(std::vector<unsigned char>& buffer, T value, uint64_t offset, bool descending);

  static void _insert_string(std::vector<unsigned char>& buffer, const pmr_string& value, uint64_t offset,
                             bool descending, uint32_t string_prefix_length);
};
}  // namespace hyrise
