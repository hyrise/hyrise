#include "column_pruning_utils.hpp"

#include <cstdlib>
#include <optional>
#include <vector>

namespace hyrise {

std::vector<ColumnID> column_ids_after_pruning(const size_t original_table_column_count,
                                               const std::vector<ColumnID>& pruned_column_ids) {
  DebugAssert(std::is_sorted(pruned_column_ids.begin(), pruned_column_ids.end()),
              "Expected a sorted vector of pruned chunk IDs.");
  DebugAssert(pruned_column_ids.empty() || pruned_column_ids.back() < original_table_column_count,
              "Largest pruned column ID is too large.");
  DebugAssert(pruned_column_ids.size() <= original_table_column_count,
              "List of pruned chunks longer than chunks in actual table.");

  auto column_id_mapping = std::vector<ColumnID>(original_table_column_count, INVALID_COLUMN_ID);
  auto column_pruned_bitvector = std::vector<bool>(original_table_column_count);

  // Fill the bitvector
  for (const auto& pruned_column_id : pruned_column_ids) {
    column_pruned_bitvector[pruned_column_id] = true;
  }

  // Calculate new column ids
  auto next_updated_column_id = ColumnID{0};
  for (auto column_index = ColumnID{0}; column_index < column_pruned_bitvector.size(); ++column_index) {
    if (!column_pruned_bitvector[column_index]) {
      column_id_mapping[column_index] = next_updated_column_id;
      ++next_updated_column_id;
    }
  }
  return column_id_mapping;
}

std::vector<ChunkID> chunk_ids_after_pruning(const size_t original_table_chunk_count,
                                             const std::vector<ChunkID>& pruned_chunk_ids) {
  DebugAssert(std::is_sorted(pruned_chunk_ids.begin(), pruned_chunk_ids.end()),
              "Expected a sorted vector of pruned chunk IDs.");
  DebugAssert(pruned_chunk_ids.empty() || pruned_chunk_ids.back() < original_table_chunk_count,
              "Largest pruned chunk ID is too large.");
  DebugAssert(pruned_chunk_ids.size() <= original_table_chunk_count,
              "List of pruned chunks longer than chunks in actual table.");

  auto chunk_id_mapping = std::vector<ChunkID>(original_table_chunk_count, INVALID_CHUNK_ID);
  auto pruned_chunk_ids_iter = pruned_chunk_ids.begin();

  // Calculate new chunk ids
  auto next_updated_chunk_id = ChunkID{0};
  for (auto chunk_index = ChunkID{0}; chunk_index < original_table_chunk_count; ++chunk_index) {
    if (pruned_chunk_ids_iter != pruned_chunk_ids.end() && chunk_index == *pruned_chunk_ids_iter) {
      ++pruned_chunk_ids_iter;
      continue;
    }

    chunk_id_mapping[chunk_index] = next_updated_chunk_id;
    ++next_updated_chunk_id;
  }
  return chunk_id_mapping;
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
