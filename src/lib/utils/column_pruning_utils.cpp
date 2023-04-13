#include "column_pruning_utils.hpp"

#include <cstdlib>
#include <optional>
#include <vector>

namespace hyrise {

std::vector<std::optional<ColumnID>> column_ids_after_pruning(const size_t original_table_column_count,
                                                              const std::vector<ColumnID>& pruned_column_ids) {
  std::vector<std::optional<ColumnID>> column_id_mapping(original_table_column_count);
  std::vector<bool> column_pruned_bitvector(original_table_column_count);

  // Fill the bitvector
  for (const auto& pruned_column_id : pruned_column_ids) {
    column_pruned_bitvector[pruned_column_id] = true;
  }

  // Calculate new column ids
  auto next_updated_column_id = ColumnID{0};
  for (auto column_index = ColumnID{0}; column_index < column_pruned_bitvector.size(); ++column_index) {
    if (!column_pruned_bitvector[column_index]) {
      column_id_mapping[column_index] = next_updated_column_id++;
    }
  }
  return column_id_mapping;
}

std::vector<std::optional<ChunkID>> chunk_ids_after_pruning(const size_t original_table_chunk_count,
                                                            const std::vector<ChunkID>& pruned_chunk_ids) {
  std::vector<std::optional<ChunkID>> chunk_id_mapping(original_table_chunk_count);
  std::vector<bool> chunk_pruned_bitvector(original_table_chunk_count);

  // Fill the bitvector
  for (const auto& pruned_chunk_id : pruned_chunk_ids) {
    chunk_pruned_bitvector[pruned_chunk_id] = true;
  }

  // Calculate new chunk ids
  auto next_updated_chunk_id = ChunkID{0};
  for (auto chunk_index = ChunkID{0}; chunk_index < original_table_chunk_count; ++chunk_index) {
    if (!chunk_pruned_bitvector[chunk_index]) {
      chunk_id_mapping[chunk_index] = next_updated_chunk_id++;
    }
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
