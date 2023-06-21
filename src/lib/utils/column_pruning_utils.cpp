#include "column_pruning_utils.hpp"

#include <cstdlib>
#include <optional>
#include <vector>

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

template <typename T>
std::vector<T> chunk_ids_after_pruning(const size_t initial_item_count, const std::vector<T>& pruned_item_ids) {
  // This function assume to be used solely for column and chunk pruning.
  static_assert(std::disjunction<std::is_same<T, ColumnID>, std::is_same<T, ChunkID>>(),
                "Unexpected template type passed.");

  auto INVALID_ID = T{0};
  if constexpr (std::is_same_v<T, ColumnID>) {
    INVALID_ID = INVALID_COLUMN_ID;
  } else {
    INVALID_ID = INVALID_CHUNK_ID;
  }

  DebugAssert(std::is_sorted(pruned_item_ids.begin(), pruned_item_ids.end()),
              "Expected a sorted vector of pruned chunk IDs.");
  DebugAssert(pruned_item_ids.empty() || pruned_item_ids.back() < initial_item_count,
              "Largest pruned chunk ID is too large.");
  DebugAssert(pruned_item_ids.size() <= initial_item_count,
              "List of pruned chunks longer than chunks in actual table.");

  auto id_mapping = std::vector<T>(initial_item_count, INVALID_ID);
  auto pruned_item_ids_iter = pruned_item_ids.begin();
  auto next_updated_id = T{0};
  for (auto item_index = T{0}; item_index < initial_item_count; ++item_index) {
    if (pruned_item_ids_iter != pruned_item_ids.end() && item_index == *pruned_item_ids_iter) {
      ++pruned_item_ids_iter;
      continue;
    }

    id_mapping[item_index] = next_updated_id;
    ++next_updated_id;
  }
  return id_mapping;
}

}  // namespace

namespace hyrise {

std::vector<ColumnID> pruned_column_id_mapping(const size_t original_table_column_count,
                                               const std::vector<ColumnID>& pruned_column_ids) {
  return chunk_ids_after_pruning<ColumnID>(original_table_column_count, pruned_column_ids);
}

std::vector<ChunkID> pruned_chunk_id_mapping(const size_t original_table_chunk_count,
                                             const std::vector<ChunkID>& pruned_chunk_ids) {
  return chunk_ids_after_pruning<ChunkID>(original_table_chunk_count, pruned_chunk_ids);
}

ColumnID column_id_before_pruning(const ColumnID column_id, const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected sorted vector of ColumnIDs");

  auto original_column_id = column_id;
  for (const auto& pruned_column_id : pruned_column_ids) {
    if (pruned_column_id > original_column_id) {
      return original_column_id;
    }
    ++original_column_id;
  }
  return original_column_id;
}

}  // namespace hyrise
